#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import json
import logging
import hashlib
import random
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

# Dependências internas do projeto (já existentes no seu repo)
from ai import analyze_products, IAResponse  # type: ignore
from shopee_monorepo_modules.publisher import TelegramPublisher  # type: ignore
from shopee_monorepo_modules.ev_signal import compute_ev_signal  # type: ignore
from rescue_publish import publish_with_rescue  # type: ignore
from storage import Storage  # type: ignore

# Integração opcional com keywords → categoria/emoji/hints
try:
    from config_keywords import resolve_meta as kw_resolve_meta  # type: ignore
except Exception:
    kw_resolve_meta = None  # fallback seguro

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("shopee_bot")

# ----------------------------------------------------------------------------
# Constantes
# ----------------------------------------------------------------------------
GRAPHQL_URL = "https://open-api.affiliate.shopee.com.br/graphql"
USER_AGENT = "Mozilla/5.0 (compatible; ShopeeAffiliateBot/2.0; +github-actions)"

# ----------------------------------------------------------------------------
# Sessão HTTP com retry
# ----------------------------------------------------------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "HEAD"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": USER_AGENT})
    return s

SESSION = make_session()

# ----------------------------------------------------------------------------
# Helpers de env
# ----------------------------------------------------------------------------
def getenv_required(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        sys.exit(f"ERRO: Variável de ambiente obrigatória ausente: {name}")
    return v

def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return default

def getenv_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default

def getenv_bool(name: str, default: bool = False) -> bool:
    v = str(os.getenv(name, str(int(default)))).strip().lower()
    return v in ("1", "true", "yes", "y", "sim")

# ----------------------------------------------------------------------------
# Normalização e categorização leve (fallback)
# ----------------------------------------------------------------------------
GENERIC_TOKENS = [
    r"\boriginal\b", r"\bofficial\b", r"\bnovo\b", r"\bnew\b", r"\bpromo(ção)?\b",
    r"\bfrete\s*grátis\b", r"\baproveite\b", r"\boferta\b", r"\bdesconto\b",
]

def norm_name(name: str) -> str:
    n = (name or "").lower()
    for rx in GENERIC_TOKENS:
        n = re.sub(rx, "", n, flags=re.I)
    n = re.sub(r"[^a-z0-9]+", " ", n)
    return n.strip()

def tag_categoria(name: str) -> str:
    n = (name or "").lower()
    if any(k in n for k in ["mouse", "teclado", "headset"]): return "periféricos"
    if any(k in n for k in ["smartwatch", "pulseira"]): return "wearables"
    if any(k in n for k in ["caixa de som", "bluetooth"]): return "áudio"
    if any(k in n for k in ["projetor", "mini projetor", "hy300"]): return "projetor"
    if any(k in n for k in ["air fryer", "airfryer"]): return "cozinha"
    if any(k in n for k in ["câmera", "camera", "segurança"]): return "segurança"
    if any(k in n for k in ["lençol", "jogo de cama"]): return "cama/banho"
    if any(k in n for k in ["bermuda", "calça", "blusa", "vestido", "touca", "gorro"]): return "moda"
    return "outros"

def compact_name(name: str, max_len: int = 80) -> str:
    n = (name or "").strip()
    for rx in GENERIC_TOKENS:
        n = re.sub(rx, "", n, flags=re.I)
    n = re.sub(r"\s{2,}", " ", n).strip(" -–—·")
    if len(n) > max_len:
        n = n[:max_len].rsplit(" ", 1)[0]
    return n

def remove_redundancy(text: str, product_name: str) -> str:
    t = (text or "").strip()
    if not t: return t
    base = compact_name(product_name).lower()
    t_low = t.lower()
    if base and t_low.startswith(base[: max(10, len(base)//2)]):
        t = t[len(base):].lstrip(" -—–:•")
    t = re.sub(r"\s{2,}", " ", t).strip(" -—–•")
    return t

def sanitize_copy(text: str) -> str:
    t = (text or "").strip()
    t = re.sub(r"\b(aproveite|compre\s*agora|garanta\s*(o|a)\s*sua?)\b", "", t, flags=re.I)
    t = re.sub(r"\s{2,}", " ", t).strip(" -—–•")
    return t

# ----------------------------------------------------------------------------
# Assinatura Shopee e GraphQL
# ----------------------------------------------------------------------------
def build_auth_header(partner_id: str, api_key: str, payload: Dict[str, Any]) -> str:
    ts = int(time.time())
    payload_str = json.dumps(payload, separators=(",", ":"))
    base_string = f"{partner_id}{ts}{payload_str}{api_key}"
    sign = hashlib.sha256(base_string.encode("utf-8")).hexdigest()
    return f"SHA256 Credential={partner_id}, Timestamp={ts}, Signature={sign}"

def gql_product_offer_v2(
    partner_id: str,
    api_key: str,
    *, keyword: Optional[str] = None, shop_id: Optional[int] = None,
    limit: int = 15, page: int = 1
) -> List[Dict[str, Any]]:
    assert (keyword is not None) ^ (shop_id is not None), "Forneça keyword OU shop_id"
    arg = f'keyword: "{keyword}"' if keyword else f"shopId: {int(shop_id)}"
    query = (
        "query { productOfferV2("
        f"{arg}, limit: {int(limit)}, page: {int(page)}"
        ") { nodes { itemId productName priceMin priceMax offerLink productLink shopName ratingStar sales priceDiscountRate } } }"
    )
    body = {"query": query, "variables": {}}
    headers = {
        "Authorization": build_auth_header(partner_id, api_key, body),
        "Content-Type": "application/json",
    }
    r = SESSION.post(GRAPHQL_URL, json=body, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data.get("data", {}).get("productOfferV2", {}).get("nodes", []) or []

# ----------------------------------------------------------------------------
# Entrada (keywords & shops)
# ----------------------------------------------------------------------------
def load_keywords(path: str = "keywords.txt") -> List[str]:
    if not os.path.exists(path):
        return [
            "mouse gamer", "teclado mecanico", "air fryer",
            "caixa de som bluetooth", "smartwatch", "camera de segurança",
        ]
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                out.append(s)
    return out

def load_shop_ids() -> List[int]:
    raw = os.getenv("SHOP_IDS", "").strip()
    if not raw:
        return []
    out: List[int] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if tok.isdigit():
            out.append(int(tok))
    return out

# ----------------------------------------------------------------------------
# Filtros e dedupe
# ----------------------------------------------------------------------------
def is_good(prod: Dict[str, Any], *, min_rating: float, min_sales: int, min_discount: float) -> bool:
    try:
        rating = float(prod.get("ratingStar") or 0.0)
    except Exception:
        rating = 0.0
    try:
        sales = int(prod.get("sales") or 0)
    except Exception:
        sales = 0
    try:
        disc = float(prod.get("priceDiscountRate") or 0.0)
    except Exception:
        disc = 0.0
    return (rating >= min_rating) and (sales >= min_sales) and (disc >= min_discount)

def dedupe_signature(prod: Dict[str, Any]) -> str:
    name = (prod.get("productName") or "").lower()
    shop = (prod.get("shopName") or "").lower()
    name_clean = re.sub(r"[^a-z0-9]+", " ", name)
    return f"{name_clean.strip()}__{shop.strip()}"

# ----------------------------------------------------------------------------
# Coleta híbrida (keywords + shops)
# ----------------------------------------------------------------------------
def coletar_ofertas(partner_id: str, api_key: str, keywords: List[str], shop_ids: List[int], pages: int) -> List[Dict[str, Any]]:
    ofertas: List[Dict[str, Any]] = []
    fontes: List[Dict[str, Any]] = ([{"tipo": "keyword", "valor": kw} for kw in keywords] +
                                    [{"tipo": "shopId", "valor": sid} for sid in shop_ids])
    for fonte in fontes:
        logger.info("Buscando %s='%s' ...", fonte["tipo"], fonte["valor"])
        for p in range(1, pages + 1):
            try:
                if fonte["tipo"] == "keyword":
                    nodes = gql_product_offer_v2(partner_id, api_key, keyword=str(fonte["valor"]), page=p, limit=15)
                else:
                    nodes = gql_product_offer_v2(partner_id, api_key, shop_id=int(fonte["valor"]), page=p, limit=15)
            except Exception as e:
                logger.warning("Falha na busca por %s '%s' (p%d): %s", fonte["tipo"], fonte["valor"], p, e)
                break
            for n in nodes:
                ofertas.append({
                    "itemId": n.get("itemId"),
                    "productName": (n.get("productName") or "").strip(),
                    "priceMin": n.get("priceMin"),
                    "priceMax": n.get("priceMax"),
                    "offerLink": n.get("offerLink"),
                    "productLink": n.get("productLink"),
                    "shopName": (n.get("shopName") or "").strip(),
                    "ratingStar": n.get("ratingStar"),
                    "sales": n.get("sales"),
                    "priceDiscountRate": n.get("priceDiscountRate"),
                    # marca a origem por keyword (para emoji/hints)
                    "keyword_origem": fonte["valor"] if fonte["tipo"] == "keyword" else None,
                })
            time.sleep(2)
    # dedupe por assinatura
    uniq: Dict[str, Dict[str, Any]] = {}
    for p in ofertas:
        uniq[dedupe_signature(p)] = p
    return list(uniq.values())

# ----------------------------------------------------------------------------
# IA (com fallback heurístico)
# ----------------------------------------------------------------------------
def heuristic_copies(prod: Dict[str, Any]) -> Dict[str, str]:
    n = (prod.get("productName") or "").lower()
    if "mouse" in n:
        a = "precisão no controle para jogos e trabalho"
        b = "pegada confortável e resposta rápida"
    elif "teclado" in n:
        a = "digitação precisa com resposta tátil"
        b = "conforto para longas sessões"
    elif "air fryer" in n or "airfryer" in n:
        a = "menos óleo e limpeza fácil"
        b = "preparo rápido no dia a dia"
    elif "caixa de som" in n or "bluetooth" in n:
        a = "som equilibrado para músicas e vídeos"
        b = "conexão estável sem fio"
    elif "smartwatch" in n:
        a = "alertas no pulso e monitoramento diário"
        b = "bateria para vários dias"
    elif "câmera" in n or "camera" in n:
        a = "monitoramento remoto no app"
        b = "visão ampla com imagem nítida"
    else:
        a = "benefício direto para o dia a dia"
        b = "versátil sem complicar"
    return {"texto_de_venda_a": a, "texto_de_venda_b": b}

def score_ia_or_fallback(batch: List[Dict[str, Any]]) -> IAResponse | Dict[str, Any]:
    try:
        return analyze_products(batch)
    except Exception as e:
        logger.error("IA indisponível — usando heurística (%d itens). Erro: %s", len(batch), e)
        items = []
        for p in batch:
            iid = int(p.get("itemId") or 0)
            if not iid: 
                continue
            h = heuristic_copies(p)
            items.append({
                "itemId": iid,
                "pontuacao": 70,
                "texto_de_venda_a": h["texto_de_venda_a"],
                "texto_de_venda_b": h["texto_de_venda_b"],
            })
        return {"items": items}

# ----------------------------------------------------------------------------
# Título curto (emoji + benefício)
# ----------------------------------------------------------------------------
def make_headline(product_name: str, benefit: str, *, emoji: Optional[str] = None, hint: Optional[str] = None, max_len: int = 110) -> str:
    base = compact_name(product_name, max_len=max_len)
    benefit = sanitize_copy(remove_redundancy(benefit, product_name))
    if hint and hint.lower() not in benefit.lower():
        benefit = f"{benefit} — {hint}"
    em = (emoji or "✨").strip() or "✨"
    title = f"{em} {base} — {benefit}".strip()
    title = re.sub(r"\s{2,}", " ", title).strip(" -–—•")
    if len(title) > max_len:
        title = title[:max_len].rsplit(" ", 1)[0]
    return title

# ----------------------------------------------------------------------------
# Seleção com caps, cooldown e preenchimento garantido (3 fases)
# ----------------------------------------------------------------------------
def select_with_caps_and_dedupe(
    ranked: List[Tuple[float, Dict[str, Any], Dict[str, Any]]],
    *, max_posts: int, max_share: float, db: Storage, cooldown_days: int,
    allow_no_cap_on_shortfall: bool, emergency_fill: bool,
    emergency_cooldown_factor: float, max_emergency_reposts: int
) -> List[Tuple[float, Dict[str, Any], Dict[str, Any]]]:
    """
    1) Estrita: cooldown + cap de categoria + dedupe por nome
    2) Sem cap (mantém cooldown) se faltar completar
    3) Emergência: relaxa cooldown para uma fração e limita #reposts
    """
    cap = max(1, int(max_posts * max_share)) if max_posts > 0 else 1
    selected: List[Tuple[float, Dict[str, Any], Dict[str, Any]]] = []
    cat_counts: Dict[str, int] = {}
    seen_norm: set[str] = set()

    rejections: List[Tuple[str, float, Dict[str, Any], Dict[str, Any]]] = []  # (reason, final, ia, prod)
    counters = {"cooldown": 0, "cap": 0, "dup": 0, "other": 0}

    # Passo 1 — estrito
    for final, ia_item, prod in ranked:
        if len(selected) >= max_posts:
            break
        name = prod.get("productName") or ""
        norm = norm_name(name)
        cat = tag_categoria(name)
        item_id = int(prod.get("itemId") or 0)
        if not item_id:
            counters["other"] += 1
            continue
        if not db.can_repost(item_id, cooldown_days=cooldown_days):
            counters["cooldown"] += 1
            rejections.append(("cooldown", final, ia_item, prod))
            continue
        if norm in seen_norm:
            counters["dup"] += 1
            rejections.append(("dup", final, ia_item, prod))
            continue
        if cat_counts.get(cat, 0) >= cap:
            counters["cap"] += 1
            rejections.append(("cap", final, ia_item, prod))
            continue
        selected.append((final, ia_item, prod))
        seen_norm.add(norm)
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    strict_sel = len(selected)

    # Passo 2 — sem cap (mantém cooldown) se necessário
    nocap_added = 0
    if allow_no_cap_on_shortfall and len(selected) < max_posts:
        for reason, final, ia_item, prod in rejections:
            if len(selected) >= max_posts:
                break
            if reason == "cooldown":
                continue
            item_id = int(prod.get("itemId") or 0)
            if not item_id or not db.can_repost(item_id, cooldown_days=cooldown_days):
                continue
            norm = norm_name(prod.get("productName") or "")
            if norm in seen_norm:
                continue
            selected.append((final, ia_item, prod))
            seen_norm.add(norm)
            nocap_added += 1

    # Passo 3 — emergência (relaxa cooldown e limita reposts)
    emergency_added = 0
    if emergency_fill and len(selected) < max_posts:
        relaxed_days = max(0, int(round(cooldown_days * emergency_cooldown_factor)))
        pool: List[Tuple[float, float, Dict[str, Any], Dict[str, Any]]] = []
        for reason, final, ia_item, prod in rejections:
            if reason != "cooldown":
                continue
            item_id = int(prod.get("itemId") or 0)
            if not item_id:
                continue
            last = db.last_posted_at(item_id) or 0.0
            if db.can_repost(item_id, cooldown_days=relaxed_days):
                pool.append((last, final, ia_item, prod))
        # nunca postados (last=0) primeiro; depois mais antigos
        pool.sort(key=lambda t: (0 if t[0] == 0 else 1, t[0]))
        used = 0
        for last, final, ia_item, prod in pool:
            if len(selected) >= max_posts or used >= max_emergency_reposts:
                break
            norm = norm_name(prod.get("productName") or "")
            if norm in seen_norm:
                continue
            selected.append((final, ia_item, prod))
            seen_norm.add(norm)
            used += 1
        emergency_added = used

    logger.info(
        "Seleção: strict=%d, +nocap=%d, +emergency=%d | rejeições: cooldown=%d, cap=%d, dup=%d, other=%d",
        strict_sel, nocap_added, emergency_added, counters["cooldown"], counters["cap"], counters["dup"], counters["other"]
    )
    return selected

# ----------------------------------------------------------------------------
# Publicação A/B
# ----------------------------------------------------------------------------
def pick_variant(rnd: random.Random) -> str:
    return "A" if rnd.random() < 0.5 else "B"

def publish_ranked_ab(
    pub: TelegramPublisher,
    db: Storage,
    ranked_selected: List[Tuple[float, Dict[str, Any], Dict[str, Any]]],
    *, max_posts: int, cooldown_days: int, dry_run: bool
) -> int:
    rnd = random.Random(42 + int(time.time()) // 3600)
    posted = 0

    for score, ia, p in ranked_selected:
        if posted >= max_posts:
            break
        iid = int(p.get("itemId") or 0)
        if not iid:
            continue
        pname = str(p.get("productName") or "")
        shop = (p.get("shopName") or "").strip()
        price = float(p.get("priceMin") or 0.0)
        rating = p.get("ratingStar")
        sales = p.get("sales")
        link = p.get("offerLink") or p.get("productLink") or ""

        # IA texts
        text_a = (ia or {}).get("texto_de_venda_a") or heuristic_copies(p)["texto_de_venda_a"]
        text_b = (ia or {}).get("texto_de_venda_b") or heuristic_copies(p)["texto_de_venda_b"]
        variant = pick_variant(rnd)
        benefit = text_a if variant == "A" else text_b

        # Enriquecimento por keyword → emoji + hints específicos
        emoji_override = None
        hint_kw = None
        if kw_resolve_meta:
            try:
                cat_kw, emoji_kw, hints_kw = kw_resolve_meta(pname, p.get("keyword_origem"))
                emoji_override = emoji_kw
                hint_kw = hints_kw[0] if hints_kw else None
            except Exception:
                pass

        title = make_headline(pname, benefit, emoji=emoji_override, hint=hint_kw)

        if dry_run:
            logger.info("[DRY RUN] %s | %s | R$%.2f | %s", title, shop, price, link)
            posted += 1
            db.record_post(iid, variant, message_id=None)
            continue

        try:
            ok = pub.send(
                title=title,
                price_brl=price if price else None,
                store=shop or None,
                rating=float(rating) if rating not in (None, "") else None,
                sales=int(sales) if str(sales).isdigit() else None,
                link=link,
                cta=("Ver oferta" if variant == "A" else "Abrir no app"),
                variant=variant,
                allow_preview=True,
            )
            if ok:
                posted += 1
                db.record_post(iid, variant, message_id=getattr(pub, "last_message_id", None))
        except requests.HTTPError as e:
            logger.warning("Erro HTTP ao publicar item %s: %s", iid, e)
        except re.error as e:
            logger.warning("Erro de regex ao publicar item %s: %s", iid, e)
        except Exception as e:
            logger.warning("Erro ao publicar item %s: %s", iid, e)

    return posted

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> int:
    # Env obrigatórios
    partner_id = getenv_required("SHOPEE_PARTNER_ID")
    api_key = getenv_required("SHOPEE_API_KEY")
    telegram_token = getenv_required("TELEGRAM_BOT_TOKEN")
    telegram_chat = getenv_required("TELEGRAM_CHAT_ID")

    # Env opcionais
    DB_PATH = os.getenv("DB_PATH", "data/bot.db")
    QTD_POSTS = getenv_int("QUANTIDADE_DE_POSTS_POR_EXECUCAO", 6)
    PAGES = getenv_int("PAGINAS_A_VERIFICAR", 2)
    DRY_RUN = getenv_bool("DRY_RUN", False)
    MIN_RATING = getenv_float("MIN_RATING", 4.7)
    MIN_DISCOUNT = getenv_float("MIN_DISCOUNT", 0.15)
    MIN_SALES = getenv_int("MIN_SALES_DEFAULT", 100)
    MAX_CATEGORY_SHARE = float(os.getenv("MAX_CATEGORY_SHARE", "0.5"))
    COOLDOWN_DIAS = getenv_int("COOLDOWN_REPOSTAGEM_DIAS", 5)

    # Estratégias de preenchimento garantido
    ALLOW_NO_CAP_ON_SHORTFALL = getenv_bool("ALLOW_NO_CAP_ON_SHORTFALL", True)
    EMERGENCY_FILL_ENABLED = getenv_bool("EMERGENCY_FILL_ENABLED", True)
    EMERGENCY_COOLDOWN_FACTOR = getenv_float("EMERGENCY_COOLDOWN_FACTOR", 0.6)  # 60% do cooldown
    MAX_EMERGENCY_REPOSTS = getenv_int("MAX_EMERGENCY_REPOSTS", 2)

    # Entrada
    keywords = load_keywords("keywords.txt")
    shops = load_shop_ids()

    logger.info("Coletando ofertas (GraphQL Affiliate)...")
    ofertas = coletar_ofertas(partner_id, api_key, keywords, shops, PAGES)
    logger.info("Coleta bruta: %d ofertas", len(ofertas))

    # Filtros de qualidade
    cand = [p for p in ofertas if is_good(p, min_rating=MIN_RATING, min_sales=MIN_SALES, min_discount=MIN_DISCOUNT)]
    logger.info("Candidatos após filtros de qualidade: %d", len(cand))

    # Dedupe por assinatura
    seen_sig = set()
    deduped = []
    for p in cand:
        sig = dedupe_signature(p)
        if sig in seen_sig:
            continue
        seen_sig.add(sig)
        deduped.append(p)
    logger.info("Após dedupe por assinatura: %d", len(deduped))

    if not deduped:
        logger.info("Sem candidatos após filtros. Nada a publicar.")
        return 0

    # IA por lotes
    BATCH = 10
    ia_by_id: Dict[int, Dict[str, Any]] = {}
    for i in range(0, len(deduped), BATCH):
        batch = deduped[i: i + BATCH]
        resp = score_ia_or_fallback(batch)
        items = getattr(resp, "items", None) or resp.get("items", [])
        for it in items:
            try:
                ia_by_id[int(it["itemId"])] = {
                    "texto_de_venda_a": it.get("texto_de_venda_a"),
                    "texto_de_venda_b": it.get("texto_de_venda_b"),
                    "pontuacao": float(it.get("pontuacao") or 0.0),
                }
            except Exception:
                continue

    # Ranking
    ranked: List[Tuple[float, Dict[str, Any], Dict[str, Any]]] = []
    for p in deduped:
        iid = int(p.get("itemId") or 0)
        ia = ia_by_id.get(iid) or heuristic_copies(p)
        ia_score = (ia.get("pontuacao") or 70.0)
        try:
            disc = float(p.get("priceDiscountRate") or 0.0)
        except Exception:
            disc = 0.0
        disc_n = max(0.0, min(1.0, disc))
        ev = 0.0
        try:
            ev = compute_ev_signal(p.get("shopName") or "", p.get("productName") or "")
        except Exception:
            ev = 0.0
        final = 0.45 * (ia_score / 100.0) + 0.25 * disc_n + 0.30 * ev
        ranked.append((final, ia, p))
    ranked.sort(key=lambda x: x[0], reverse=True)

    # Persistência e publisher
    db = Storage(DB_PATH)
    pub = TelegramPublisher(token=telegram_token, chat_id=telegram_chat)

    selected = select_with_caps_and_dedupe(
        ranked,
        max_posts=QTD_POSTS,
        max_share=MAX_CATEGORY_SHARE,
        db=db,
        cooldown_days=COOLDOWN_DIAS,
        allow_no_cap_on_shortfall=ALLOW_NO_CAP_ON_SHORTFALL,
        emergency_fill=EMERGENCY_FILL_ENABLED,
        emergency_cooldown_factor=EMERGENCY_COOLDOWN_FACTOR,
        max_emergency_reposts=MAX_EMERGENCY_REPOSTS,
    )
    logger.info("Selecionados (após caps/dedupe): %d", len(selected))

    posted = publish_ranked_ab(
        pub, db, selected, max_posts=QTD_POSTS, cooldown_days=COOLDOWN_DIAS, dry_run=DRY_RUN
    )

    # RESGATE se necessário
    if posted < QTD_POSTS:
        logger.warning("Ativando modo RESGATE: coletando mais itens com filtros relaxados...")
        try:
            posted += publish_with_rescue(
                pub=pub,
                db=db,
                partner_id=partner_id,
                api_key=api_key,
                already_posted=posted,
                target=QTD_POSTS,
                cooldown_days=COOLDOWN_DIAS,
                keywords=keywords,
                shops=shops,
            )
        except Exception as e:
            logger.warning("Resgate falhou: %s", e)

    logger.info("Publicações concluídas: %d", posted)
    return posted

if __name__ == "__main__":
    try:
        c = main()
        sys.exit(0 if (c is not None and c >= 0) else 1)
    except SystemExit:
        raise
    except Exception as e:
        logger.exception("Erro fatal no bot: %s", e)
        sys.exit(1)