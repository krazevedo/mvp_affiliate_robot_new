#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
shopee_bot.py — coleta ofertas via Shopee Affiliate GraphQL, ranqueia com IA + EV,
aplica guardrails de copy, dedupe por categoria e publica no Telegram com A/B.

Melhorias implementadas:
- Filtros por categoria: MIN_SALES e MIN_DISCOUNT dinâmicos via env.
- Prova de preço: "Abaixo do preço mediano de 30 dias" quando detectado no histórico.
- Formatação padronizada (título em negrito, corpo claro, rodapé, CTA única).
- A/B controlado por categoria com exploração (%).
- Telemetria gravada em tabela própria (post_telemetry).
- Limite de chamadas à IA (IA_TOP_K) e fallback heurístico para quota 429.
"""

from __future__ import annotations

import os, sys, time, json, logging, hashlib, random, re, sqlite3
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

from ai import analyze_products, IAResponse
from shopee_monorepo_modules.publisher import TelegramPublisher
from shopee_monorepo_modules.ev_signal import compute_ev_signal
from storage import Storage

# ===== logging =====
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("shopee_bot")

# ===== Config API Shopee =====
AFFILIATE_ENDPOINT = "https://open-api.affiliate.shopee.com.br/graphql"
DEFAULT_CONNECT_TIMEOUT = 8
DEFAULT_READ_TIMEOUT = 20
USER_AGENT = "OfferBot/2.0 (+https://github.com/yourrepo)"

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "HEAD"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": USER_AGENT})
    return s

SESSION = make_session()

# ===== helpers env =====
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

# ===== categorização & normalização =====
CATS = [
    ("fone/earbud/headset", r"\bfone\b|\bearpod|\bheadset|\bairpods|\bbluetooth\b|\btws\b"),
    ("mouse/teclado/periféricos", r"\bmouse\b|\bteclado\b|\bmousepad\b|\bkit gamer"),
    ("smartwatch/wearables", r"\bsmartwatch|\bwatch\b|\bmicrowear\b|\bw\d{2}\b|\bs8\b|\bseries\b"),
    ("caixa de som/speaker", r"\bcaixa\b|\bsom\b|\bspeaker\b|xtrad|inova"),
    ("projetor", r"\bprojetor|\bhy300|\bhy320\b|magcubic"),
    ("cozinha (airfryer etc.)", r"\bair ?fry|\bfritadeir"),
    ("câmera/segurança", r"\bc[aâ]mera\b|\bespi\b"),
    ("papelaria", r"\bcaneta|\bmarca texto|\bapontador"),
    ("outros", r".*"),
]

def tag_categoria(name: str) -> str:
    n = (name or "").lower()
    for cat, pat in CATS:
        if re.search(pat, n):
            return cat
    return "outros"

def norm_name(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for tok in [" com fio", " sem fio", " led", " rgb", " bluetooth", " original", " gamer"]:
        s = s.replace(tok, "")
    return s

# ===== thresholds por categoria =====
def parse_map_env(env_name: str) -> Dict[str, float]:
    raw = os.getenv(env_name, "").strip()
    out: Dict[str, float] = {}
    if not raw:
        return out
    for part in raw.split(","):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        k = k.strip().lower()
        try:
            out[k] = float(v.strip())
        except Exception:
            continue
    return out

MIN_SALES_DEFAULT = getenv_int("MIN_SALES_DEFAULT", 100)
MIN_SALES_BY_CAT = {k: int(v) for k, v in parse_map_env("MIN_SALES_BY_CAT").items()}  # ex: "cozinha:80,gamer:50"
MIN_DISCOUNT_DEFAULT = getenv_float("MIN_DISCOUNT", 0.15)
MIN_DISCOUNT_BY_CAT = parse_map_env("MIN_DISCOUNT_BY_CAT")  # ex: "gadgets:0.2,cozinha:0.15,audio:0.1"

def get_min_sales(cat: str) -> int:
    return int(MIN_SALES_BY_CAT.get(cat.lower(), MIN_SALES_DEFAULT))

def get_min_discount(cat: str) -> float:
    return float(MIN_DISCOUNT_BY_CAT.get(cat.lower(), MIN_DISCOUNT_DEFAULT))

# ===== Assinatura Shopee (GraphQL) =====
def _build_auth_header(partner_id: int, api_key: str, payload_str: str, ts: Optional[int] = None) -> Tuple[str, int]:
    timestamp = int(ts or time.time())
    base_string = f"{partner_id}{timestamp}{payload_str}{api_key}"
    signature = hashlib.sha256(base_string.encode("utf-8")).hexdigest()
    return f"SHA256 Credential={partner_id}, Timestamp={timestamp}, Signature={signature}", timestamp

def graphql_product_offer_v2(partner_id: int, api_key: str, *, keyword: Optional[str] = None, shop_id: Optional[int] = None, limit: int = 15, page: int = 1) -> Dict[str, Any]:
    assert (keyword is not None) ^ (shop_id is not None), "Forneça keyword OU shop_id"
    params = f'keyword: "{keyword}"' if keyword is not None else f"shopId: {int(shop_id)}"
    query = (
        "query { productOfferV2("
        + f"{params}, limit: {int(limit)}, page: {int(page)}"
        + ") { nodes { itemId productName priceMin priceMax offerLink productLink shopName ratingStar sales priceDiscountRate } } }"
    )
    body = {"query": query, "variables": {}}
    payload = json.dumps(body, separators=(",", ":"))
    auth, _ = _build_auth_header(partner_id, api_key, payload)
    headers = {"Authorization": auth, "Content-Type": "application/json"}
    r = SESSION.post(AFFILIATE_ENDPOINT, data=payload, headers=headers, timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT))
    r.raise_for_status()
    return r.json()

def verificar_link_ativo(url: str) -> bool:
    if not url:
        return False
    try:
        r = SESSION.head(url, allow_redirects=True, timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT), headers={"User-Agent": USER_AGENT})
        if 200 <= r.status_code < 400:
            return True
    except Exception:
        pass
    try:
        r = SESSION.get(url, allow_redirects=True, timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT), headers={"User-Agent": USER_AGENT})
        return (200 <= r.status_code < 400) and ("O produto não existe" not in (r.text or ""))
    except Exception:
        return False

# ===== Coleta, blocklist e filtros =====
def carregar_keywords(path: str = "keywords.txt") -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            kws = [ln.strip() for ln in f if ln.strip()]
            if kws:
                return kws
    except FileNotFoundError:
        pass
    return [s.strip() for s in os.getenv("SHOPEE_KEYWORDS", "gadgets,casa,beleza").split(",") if s.strip()]

def carregar_lojas_env() -> List[int]:
    raw=os.getenv("SHOPEE_SHOP_IDS","369632653, 288420684, 286277644, 1157280425, 1315886500, 349591196, 886950101").strip()
    if not raw:
        return []
    out: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            logger.warning("ShopId inválido: %r", part)
    return out

def get_blocklist_patterns() -> List[re.Pattern]:
    raw = os.getenv("BLOCKLIST_TERMS", "espi,espiã,pmpo,chatgpt,4k,i12")
    terms = [t.strip() for t in raw.split(",") if t.strip()]
    return [re.compile(rf"\b{re.escape(t)}\b", re.IGNORECASE) for t in terms]

def is_blocked(name: str, patterns: List[re.Pattern]) -> bool:
    if not name:
        return False
    return any(p.search(name) for p in patterns)

def coletar_ofertas(partner_id: int, api_key: str, *, keywords: List[str], shop_ids: List[int], paginas: int, itens_por_pagina: int) -> List[Dict[str, Any]]:
    ofertas: List[Dict[str, Any]] = []
    fontes = [{"tipo": "keyword", "valor": kw} for kw in keywords] + [{"tipo": "shopId", "valor": sid} for sid in shop_ids]
    bl = get_blocklist_patterns()
    for fonte in fontes:
        logger.info("Buscando %s=%r ...", fonte["tipo"], fonte["valor"])
        for page in range(1, paginas + 1):
            try:
                if fonte["tipo"] == "keyword":
                    data = graphql_product_offer_v2(partner_id, api_key, keyword=str(fonte["valor"]), limit=itens_por_pagina, page=page)
                else:
                    data = graphql_product_offer_v2(partner_id, api_key, shop_id=int(fonte["valor"]), limit=itens_por_pagina, page=page)
            except requests.HTTPError as he:
                msg = getattr(he.response, "text", str(he))
                logger.warning("HTTPError page=%s: %s", page, msg)
                break
            except Exception as e:
                logger.warning("Erro req page=%s: %s", page, e)
                break

            if "errors" in data and data["errors"]:
                logger.warning("Erro GraphQL: %s", data["errors"])
                break

            nodes = (((data or {}).get("data") or {}).get("productOfferV2") or {}).get("nodes", []) or []
            if not nodes:
                logger.info("Sem resultados (page=%s).", page)
                break

            for p in nodes:
                name = p.get("productName") or ""
                if is_blocked(name, bl):
                    continue
                try:
                    iid = int(p.get("itemId"))
                except Exception:
                    continue
                if not verificar_link_ativo(p.get("productLink")):
                    continue
                ofertas.append(
                    {
                        "itemId": iid,
                        "productName": name,
                        "priceMin": p.get("priceMin"),
                        "priceMax": p.get("priceMax"),
                        "offerLink": p.get("offerLink"),
                        "productLink": p.get("productLink"),
                        "shopName": p.get("shopName"),
                        "ratingStar": p.get("ratingStar"),
                        "sales": p.get("sales"),
                        "priceDiscountRate": p.get("priceDiscountRate"),
                    }
                )
            time.sleep(2)  # respeitar rate
    # dedupe por itemId
    dedup = {it["itemId"]: it for it in ofertas}
    return list(dedup.values())

# ===== Guardrails de copy =====
PRICE_PAT = re.compile(r"(r\$\s?\d+[\.,]?\d*)|(%\s?off)", re.IGNORECASE)
STAR_PAT = re.compile(r"(\d[\.,]?\d\s*estrelas?)|(avalia[cç][aã]o\s*\d[\.,]?\d)", re.IGNORECASE)
SALES_PAT = re.compile(r"(\d+\+?\s*vendas?)", re.IGNORECASE)
CTA_PAT = re.compile(r"\b(aproveite(?: agora)?|garanta a sua|ver oferta|compre agora)\b[\.!]?", re.IGNORECASE)
URGENCY_TAIL = re.compile(r"(agora|enquanto dura|estoque limitado|últimas unidades|por tempo limitado)[\.!]?$", re.IGNORECASE)

SPEC_PATTERNS = [
    (re.compile(r"\b(\d{3,5})\s*dpi\b", re.IGNORECASE), lambda m: f"{m.group(1)} DPI"),
    (re.compile(r"\b(ip(?:6[7-9]|x?8))\b", re.IGNORECASE), lambda m: m.group(1).upper()),
    (re.compile(r"\b(\d+)\s*l\b", re.IGNORECASE), lambda m: f"{m.group(1)}L"),
    (re.compile(r"\b360\b"), lambda m: "rotação 360°"),
    (re.compile(r"\bbluetooth\b", re.IGNORECASE), lambda m: "Bluetooth"),
    (re.compile(r"\banc\b|\bnoise\b", re.IGNORECASE), lambda m: "cancelamento de ruído"),
]

def derive_hint(name: str) -> Optional[str]:
    n = name or ""
    for pat, fmt in SPEC_PATTERNS:
        m = pat.search(n)
        if m:
            try:
                return fmt(m)
            except Exception:
                continue
    return None

def sanitize_copy(text: str) -> str:
    t = text or ""
    t = PRICE_PAT.sub("", t)
    t = STAR_PAT.sub("", t)
    t = SALES_PAT.sub("", t)
    t = CTA_PAT.sub("", t)
    t = re.sub(r"\s{2,}", " ", t).strip()
    t = re.sub(r"[\.\!\?]{2,}$", ".", t)
    return t

def enforce_style(text: str, product_name: str, category: str, hint: Optional[str] = None) -> str:
    t = sanitize_copy(text)
    pn = (product_name or "").strip()
    if pn and pn[:8].lower() not in t.lower() and (category or "").lower() not in t.lower():
        t = f"{pn}: {t}"
    if hint and hint.lower() not in t.lower() and len(t) < 120:
        if ": " in t[:60]:
            t = t.replace(": ", f": {hint} — ", 1)
        else:
            t = f"{t} — {hint}"
    if not URGENCY_TAIL.search(t) and len(t) < 90:
        t = (t + " Aproveite.").strip()
    if len(t) > 170:
        t = t[:165].rsplit(" ", 1)[0] + "..."
    t = re.sub(r"!{2,}", "!", t)
    return t

# ===== IA e heurística =====
def heuristic_score(prod: Dict[str, Any], db_path: str) -> float:
    try:
        disc = float(prod.get("priceDiscountRate") or 0.0)
    except Exception:
        disc = 0.0
    try:
        rating = float(prod.get("ratingStar") or 0.0)
    except Exception:
        rating = 0.0
    rating_n = max(0.0, min(1.0, (rating - 4.5) / 0.5))  # 4.5->0, 5.0->1
    ev = compute_ev_signal(db_path, item_id=int(prod.get("itemId") or 0), product_name=prod.get("productName", ""), shop_name=prod.get("shopName"))
    return 0.45 * disc + 0.35 * rating_n + 0.20 * ev

def heuristic_copies(prod: Dict[str, Any]) -> Dict[str, Any]:
    name = str(prod.get("productName") or "Oferta").strip()
    cat = tag_categoria(name)
    hint = derive_hint(name)
    base = f"{name}"
    if hint:
        base += f": {hint}"
    a = enforce_style(f"{base} para o dia a dia com ótimo custo-benefício.", name, cat, hint=hint)
    b = enforce_style(f"{base} ideal para quem busca praticidade no uso diário.", name, cat, hint=hint)
    return {"texto_de_venda_a": a, "texto_de_venda_b": b}

# ===== preço mediano 30d =====
def _detect_price_table(con) -> Optional[Tuple[str, str, str]]:
    # retorna (table, item_col, price_col, time_col) — time_col epoch ou ISO8601
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    item_candidates = ["item_id", "itemId", "product_id"]
    price_candidates = ["price", "priceMin", "price_min"]
    time_candidates = ["timestamp", "ts", "created_at", "time", "dt", "posted_at"]
    for t in tables:
        try:
            cur.execute(f"PRAGMA table_info({t})")
            cols = [r[1] for r in cur.fetchall()]
            item_col = next((c for c in item_candidates if c in cols), None)
            price_col = next((c for c in price_candidates if c in cols), None)
            time_col = next((c for c in time_candidates if c in cols), None)
            if item_col and price_col and time_col:
                return (t, item_col, price_col, time_col)
        except Exception:
            continue
    return None

def is_below_30d_median(db_path: str, item_id: int, current_price: float) -> bool:
    try:
        con = sqlite3.connect(db_path)
    except Exception:
        return False
    try:
        det = _detect_price_table(con)
        if not det:
            return False
        t, item_col, price_col, time_col = det
        # Seleciona últimos 30 dias (tanto epoch quanto texto)
        q = f"""
        SELECT {price_col}
        FROM {t}
        WHERE {item_col}=?
          AND (
            (typeof({time_col})='integer' AND {time_col} >= strftime('%s','now','-30 day'))
            OR
            (typeof({time_col})!='integer' AND datetime({time_col}) >= datetime('now','-30 day'))
          )
        ORDER BY {time_col} DESC
        """
        cur = con.cursor()
        cur.execute(q, (item_id,))
        prices = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
        if len(prices) < 5:
            return False
        prices_sorted = sorted(prices)
        n = len(prices_sorted)
        if n % 2 == 1:
            median = prices_sorted[n//2]
        else:
            median = 0.5*(prices_sorted[n//2 - 1] + prices_sorted[n//2])
        return (float(current_price) or 0.0) < median * 0.98  # 2% abaixo do mediano
    except Exception as e:
        return False
    finally:
        con.close()

# ===== Seleção final =====
def select_with_caps_and_dedupe(ranked: List[Tuple[float, Dict[str, Any], Dict[str, Any]]], *, max_posts: int, max_share: float) -> List[Tuple[float, Dict[str, Any], Dict[str, Any]]]:
    cap = max(1, int(max_posts * max_share))
    chosen: List[Tuple[float, Dict[str, Any], Dict[str, Any]]] = []
    cat_counts: Dict[str, int] = {}
    seen_norm: set[str] = set()
    for item in ranked:
        if len(chosen) >= max_posts:
            break
        _, ia_item, prod = item
        cat = tag_categoria(prod.get("productName") or "")
        norm = norm_name(prod.get("productName") or "")
        if norm in seen_norm:
            continue
        if cat_counts.get(cat, 0) >= cap:
            continue
        chosen.append(item)
        seen_norm.add(norm)
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    return chosen

# ===== A/B controlado por categoria =====
def parse_variant_map(env_name: str) -> Dict[str, str]:
    raw = os.getenv(env_name, "").strip()
    out: Dict[str, str] = {}
    if not raw:
        return out
    for part in raw.split(","):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        out[k.strip().lower()] = v.strip().upper()[:1] if v.strip() else "A"
    return out

AB_DEFAULT_VARIANT_BY_CAT = parse_variant_map("AB_DEFAULT_VARIANT_BY_CAT")  # ex: "cozinha:A,gamer:B"
AB_EXPLORE_PCT = max(0.0, min(1.0, float(os.getenv("AB_EXPLORE_PCT", "0.3"))))

def choose_variant_for_category(cat: str, rng: random.Random) -> str:
    # Explora com probabilidade AB_EXPLORE_PCT; caso contrário usa padrão do mapa (default A)
    if rng.random() < AB_EXPLORE_PCT:
        return rng.choice(["A", "B"])
    return AB_DEFAULT_VARIANT_BY_CAT.get(cat.lower(), "A")

def count_emojis(s: str) -> int:
    # Contagem simples de emojis (faixas comuns)
    if not s:
        return 0
    return len(re.findall(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]", s))

def record_post_telemetry(db_path: str, *, message_id: str, item_id: int, category: str, copy_len: int, cta_used: str, emoji_used: int, below_median_30d: bool, variant: str):
    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS post_telemetry (
            message_id TEXT,
            item_id INTEGER,
            created_at INTEGER DEFAULT (strftime('%s','now')),
            category TEXT,
            copy_len INTEGER,
            cta_used TEXT,
            emoji_used INTEGER,
            below_median_30d INTEGER,
            variant TEXT
        )
        """)
        cur.execute("""
        INSERT INTO post_telemetry (message_id, item_id, category, copy_len, cta_used, emoji_used, below_median_30d, variant)
        VALUES (?,?,?,?,?,?,?,?)
        """, (message_id, item_id, category, copy_len, cta_used, int(bool(below_median_30d)), variant))
        con.commit()
        con.close()
    except Exception as e:
        logger.warning("Falha ao registrar telemetria: %s", e)

# ===== Publicação (A/B) =====
def publish_ranked_ab(pub: TelegramPublisher, db: Storage, ranked: List[Tuple[float, Dict[str, Any], Dict[str, Any]]], *, max_posts: int, cooldown_days: int, dry_run: bool, db_path: str) -> int:
    posted = 0
    campaign = time.strftime("%Y%m%d")
    rng = random.Random()
    for final_score, ia_item, prod in ranked:
        item_id = int(ia_item["itemId"] if isinstance(ia_item, dict) else getattr(ia_item, "itemId", 0) or 0)
        if not item_id:
            continue
        if not db.can_repost(item_id, cooldown_days=cooldown_days):
            logger.info("Cooldown ativo para item %s — pulando", item_id)
            continue

        pname = str(prod.get("productName") or "")
        cat = tag_categoria(pname)
        hint = derive_hint(pname)

        # seleciona variante com exploração controlada
        variant = choose_variant_for_category(cat, rng)

        text_a = ia_item["texto_de_venda_a"] if isinstance(ia_item, dict) else ia_item.texto_de_venda_a
        text_b = ia_item["texto_de_venda_b"] if isinstance(ia_item, dict) else ia_item.texto_de_venda_b
        raw_text = text_a if variant == "A" else text_b

        texto = enforce_style(raw_text, pname, cat, hint=hint)

        # Rodapé e flags
        try:
            price = float(prod.get("priceMin") or 0.0)
        except Exception:
            price = 0.0
        shop = str(prod.get("shopName") or "")
        rating = prod.get("ratingStar")
        offer = str(prod.get("offerLink") or prod.get("productLink") or "")
        sales = prod.get("sales")
        discount_rate = prod.get("priceDiscountRate")

        below_med = is_below_30d_median(db_path, item_id, price)
        high_trust = (isinstance(rating, (int, float)) and float(rating) >= 4.8) and (isinstance(sales, int) and sales >= 100)

        sub_id = f"{item_id}-{variant}-{time.strftime('%Y%m%d')}"
        cta_text = "🔗 Ver oferta" if variant == "A" else "🔗 Abrir no app"

        msg = pub.build_message(
            product_name=pname,
            texto_ia=texto,
            price=price,
            shop=shop,
            offer=offer,
            rating=float(rating) if rating not in (None, "") else None,
            discount_rate=float(discount_rate) if discount_rate not in (None, "") else None,
            sales=int(sales) if isinstance(sales, (int, float, str)) and str(sales).isdigit() else None,
            badge=None,
            campaign=campaign,
            sub_id=sub_id,
            cta_text=cta_text,
            below_median_30d=below_med,
            high_trust=high_trust,
        )

        if dry_run:
            logger.info("[DRY RUN][%s] Postaria item %s | score=%.2f | %s", variant, item_id, final_score, offer)
            db.record_post(item_id, variant=variant, message_id=f"dryrun-{int(time.time())}")
            emoji_ct = count_emojis(msg)
            record_post_telemetry(db_path, message_id=f"dryrun-{int(time.time())}", item_id=item_id, category=cat, copy_len=len(texto), cta_used=cta_text, emoji_used=emoji_ct, below_median_30d=below_med, variant=variant)
            posted += 1
        else:
            try:
                mid = pub.send_message(msg)
                if mid:
                    db.record_post(item_id, variant=variant, message_id=str(mid))
                    emoji_ct = count_emojis(msg)
                    record_post_telemetry(db_path, message_id=str(mid), item_id=item_id, category=cat, copy_len=len(texto), cta_used=cta_text, emoji_used=emoji_ct, below_median_30d=below_med, variant=variant)
                    posted += 1
                    logger.info("Publicado [%s] item %s | score=%.2f | message_id=%s", variant, item_id, final_score, mid)
                else:
                    logger.warning("Falha ao publicar item %s — sem message_id", item_id)
            except Exception as e:
                logger.warning("Erro ao publicar item %s: %s", item_id, e)
        if posted >= max_posts:
            break
    return posted

# ===== Filtro candidatos com mínimos por categoria =====
def filter_candidates(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in products:
        name = p.get("productName") or ""
        cat = tag_categoria(name)
        min_sales = get_min_sales(cat)
        min_disc = get_min_discount(cat)
        try:
            rating = float(p.get("ratingStar") or 0.0)
        except Exception:
            rating = 0.0
        try:
            disc = float(p.get("priceDiscountRate") or 0.0)
        except Exception:
            disc = 0.0
        try:
            sales = int(p.get("sales") or 0)
        except Exception:
            sales = 0
        min_rating = float(os.getenv("MIN_RATING", "4.7"))
        if rating >= min_rating and disc >= min_disc and sales >= min_sales:
            out.append(p)
    return out

# ===== Main =====
def main():
    PARTNER_ID_STR = getenv_required("SHOPEE_PARTNER_ID")
    API_KEY = getenv_required("SHOPEE_API_KEY")
    _ = getenv_required("GEMINI_API_KEY")  # ainda exigimos; se IA_ENABLED=0, não será usada
    TELEGRAM_BOT_TOKEN = getenv_required("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = getenv_required("TELEGRAM_CHAT_ID")
    try:
        PARTNER_ID = int(PARTNER_ID_STR)
    except Exception:
        sys.exit("ERRO: SHOPEE_PARTNER_ID inválido (não numérico).")

    QTD_POSTS = getenv_int("QUANTIDADE_DE_POSTS_POR_EXECUCAO", 3)
    PAGINAS = getenv_int("PAGINAS_A_VERIFICAR", 2)
    ITENS_POR_PAGINA = getenv_int("ITENS_POR_PAGINA", 15)
    MIN_IA_SCORE = getenv_float("MIN_IA_SCORE", 65.0)
    COOLDOWN_DIAS = getenv_int("COOLDOWN_REPOSTAGEM_DIAS", 5)
    DRY_RUN = getenv_bool("DRY_RUN", False)
    MAX_CATEGORY_SHARE = float(os.getenv("MAX_CATEGORY_SHARE", "0.4"))
    DB_PATH = os.getenv("DB_PATH", "data/bot.db")

    IA_TOP_K = getenv_int("IA_TOP_K", 10)
    IA_BATCH_SIZE = getenv_int("IA_BATCH_SIZE", min(IA_TOP_K, 10) or 10)
    IA_ENABLED = getenv_bool("IA_ENABLED", True)

    keywords = carregar_keywords()
    shop_ids = carregar_lojas_env()

    db = Storage()
    pub = TelegramPublisher(bot_token=TELEGRAM_BOT_TOKEN, chat_id=TELEGRAM_CHAT_ID)

    logger.info("Coletando ofertas (GraphQL Affiliate)...")
    gross = coletar_ofertas(PARTNER_ID, API_KEY, keywords=keywords, shop_ids=shop_ids, paginas=PAGINAS, itens_por_pagina=ITENS_POR_PAGINA)
    logger.info("Coleta bruta: %d ofertas", len(gross))

    # Persistência (preço/histórico)
    for p in gross:
        try:
            db.upsert_product(
                {
                    "itemId": p.get("itemId"),
                    "item_id": p.get("itemId"),
                    "name": p.get("productName"),
                    "productLink": p.get("productLink"),
                    "ratingStar": p.get("ratingStar"),
                    "sales": p.get("sales"),
                    "priceMin": p.get("priceMin"),
                    "priceMax": p.get("priceMax"),
                    "priceDiscountRate": p.get("priceDiscountRate"),
                }
            )
            if p.get("priceMin") is not None:
                db.add_price_point(int(p["itemId"]), float(p["priceMin"]))
        except Exception as e:
            logger.warning("Falha ao persistir item %s: %s", p.get("itemId"), e)

    candidates = filter_candidates(gross)
    logger.info("Candidatos após filtros dinâmicos: %d", len(candidates))
    if not candidates:
        logger.warning("Sem candidatos após filtros. Encerrando.")
        return 0

    # ===== IA quota-safe =====
    ia_results: List[Dict[str, Any]] = []
    prelim = [(heuristic_score(p, DB_PATH), p) for p in candidates]
    prelim.sort(key=lambda x: x[0], reverse=True)
    top_for_ia = [p for _, p in prelim[:IA_TOP_K]] if IA_ENABLED else []

    if IA_ENABLED and top_for_ia:
        for i in range(0, len(top_for_ia), IA_BATCH_SIZE):
            batch = top_for_ia[i : i + IA_BATCH_SIZE]
            try:
                resp: IAResponse = analyze_products(batch)
                ia_results.extend([x.model_dump() for x in resp.analise_de_produtos])
            except Exception as e:
                logger.warning("IA indisponível para o lote %s — heurística (%s itens). Erro: %s", (i // IA_BATCH_SIZE) + 1, len(batch), e)
                for p in batch:
                    iid = int(p["itemId"])
                    h = heuristic_copies(p)
                    pre = next((s for s, pp in prelim if pp is p), 0.4)
                    score = int(55 + (82 - 55) * max(0.0, min(1.0, pre)))
                    ia_results.append({"itemId": iid, "pontuacao": score, "texto_de_venda_a": h["texto_de_venda_a"], "texto_de_venda_b": h["texto_de_venda_b"]})

    remaining = [p for p in candidates if p not in top_for_ia]
    for p in remaining:
        iid = int(p["itemId"])
        h = heuristic_copies(p)
        pre = heuristic_score(p, DB_PATH)
        score = int(58 + (78 - 58) * max(0.0, min(1.0, pre)))
        ia_results.append({"itemId": iid, "pontuacao": score, "texto_de_venda_a": h["texto_de_venda_a"], "texto_de_venda_b": h["texto_de_venda_b"]})

    ia_by_id = {int(x["itemId"]): x for x in ia_results if str(x.get("itemId", "")).isdigit()}

    # ===== Blend de score final (IA + desconto + EV) =====
    ranked: List[Tuple[float, Dict[str, Any], Dict[str, Any]]] = []
    for p in candidates:
        iid = int(p["itemId"])
        ia = ia_by_id.get(iid)
        if not ia:
            continue
        ia_score = float(ia.get("pontuacao", 0))
        if ia_score < MIN_IA_SCORE:
            continue
        ev = compute_ev_signal(DB_PATH, item_id=iid, product_name=p.get("productName", ""), shop_name=p.get("shopName"))
        ia_n = ia_score / 100.0
        disc_n = float(p.get("priceDiscountRate") or 0.0)
        final = 0.45 * ia_n + 0.25 * disc_n + 0.30 * ev
        ranked.append((final, ia, p))
    ranked.sort(key=lambda x: x[0], reverse=True)

    selected = select_with_caps_and_dedupe(ranked, max_posts=QTD_POSTS, max_share=float(os.getenv("MAX_CATEGORY_SHARE", "0.5")))
    logger.info("Selecionados (após caps/dedupe): %d", len(selected))

    posted = publish_ranked_ab(pub, db, selected, max_posts=QTD_POSTS, cooldown_days=getenv_int("COOLDOWN_REPOSTAGEM_DIAS", 5), dry_run=getenv_bool("DRY_RUN", False), db_path=DB_PATH)
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
