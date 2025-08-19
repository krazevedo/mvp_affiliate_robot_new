#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, sys, time, json, logging, hashlib, random, re, sqlite3
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import requests
from requests.adapters import HTTPAdapter, Retry

from ai import analyze_products, IAResponse
from shopee_monorepo_modules.publisher import TelegramPublisher
from shopee_monorepo_modules.ev_signal import compute_ev_signal
from rescue_publish import publish_with_rescue
from storage import Storage

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("shopee_bot")

AFFILIATE_ENDPOINT = "https://open-api.affiliate.shopee.com.br/graphql"
DEFAULT_CONNECT_TIMEOUT = 8
DEFAULT_READ_TIMEOUT = 20
USER_AGENT = "OfferBot/1.7 (+https://github.com/yourrepo)"

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
    ("mouse/teclado/periféricos", r"\bmouse\b|\bteclado\b|\bmousepad\b|\bkit gamer"),
    ("smartwatch/wearables", r"\bsmartwatch|\bwatch\b|\bmicrowear\b|\bw\d{2}\b|\bs8\b|\bseries\b"),
    ("caixa de som/speaker", r"\bcaixa\b|\bsom\b|\bspeaker\b|xtrad|inova"),
    ("projetor", r"\bprojetor|\bhy300|\bhy320\b|magcubic"),
    ("cozinha (airfryer etc.)", r"\bair ?fry|\bfritadeir"),
    ("câmera/segurança", r"\bc[aâ]mera\b|\bespi\b"),
    ("papelaria", r"\bcaneta|\bmarca texto|\bapontador"),
    ("outros", r".*"),
]

EMOJI_BY_CAT = {
    "mouse/teclado/periféricos": "🎮",
    "smartwatch/wearables": "⌚",
    "caixa de som/speaker": "🔊",
    "projetor": "📽️",
    "cozinha (airfryer etc.)": "🍟",
    "câmera/segurança": "📹",
    "papelaria": "🖊️",
    "outros": "✨",
}

def tag_categoria(name: str) -> str:
    n = (name or "").lower()
    for cat, pat in CATS:
        if re.search(pat, n):
            return cat
    return "outros"

GENERIC_TOKENS = {"gamer","bluetooth","original","usb","com fio","sem fio","led","rgb","headset","fone","mouse","teclado"}

def norm_name(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for tok in [" com fio", " sem fio", " led", " rgb", " bluetooth", " original", " gamer"]:
        s = s.replace(tok, "")
    return s

def extract_brand(name: str) -> Optional[str]:
    for tok in re.findall(r"[A-ZÁÂÃÉÊÍÓÔÕÚÇ][\w\-]{2,}", name or ""):
        tl = tok.lower()
        if tl not in GENERIC_TOKENS and not tl.isdigit():
            return tok
    parts = (name or "").split()
    return parts[-1] if parts else None

def extract_dpi(name: str) -> Optional[int]:
    m = re.search(r"(\d{3,5})\s*DPI", name or "", re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

def extract_buttons(name: str) -> Optional[int]:
    m = re.search(r"(\d+)\s*bot", name or "", re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

def make_signature_key(name: str) -> Optional[Tuple[str, Optional[str], Optional[int], Optional[int]]]:
    cat = tag_categoria(name)
    brand = extract_brand(name or "")
    dpi = extract_dpi(name or "")
    btn = extract_buttons(name or "")
    return (cat, brand, dpi, btn)

# ===== Assinatura Shopee (GraphQL) =====
def _build_auth_header(partner_id: int, api_key: str, payload_str: str, ts: Optional[int] = None) -> Tuple[str, int]:
    timestamp = int(ts or time.time())
    base_string = f"{partner_id}{timestamp}{payload_str}{api_key}"
    signature = hashlib.sha256(base_string.encode("utf-8")).hexdigest()
    return f"SHA256 Credential={partner_id}, Timestamp={timestamp}, Signature={signature}", timestamp

def graphql_product_offer_v2(
    partner_id: int,
    api_key: str,
    *,
    keyword: Optional[str] = None,
    shop_id: Optional[int] = None,
    limit: int = 15,
    page: int = 1,
) -> Dict[str, Any]:
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
    r = SESSION.post(
        AFFILIATE_ENDPOINT,
        data=payload,
        headers=headers,
        timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT),
    )
    r.raise_for_status()
    return r.json()

def verificar_link_ativo(url: str) -> bool:
    if not url:
        return False
    try:
        r = SESSION.head(url, allow_redirects=True, timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT),
                         headers={"User-Agent": USER_AGENT})
        if 200 <= r.status_code < 400:
            return True
    except Exception:
        pass
    try:
        r = SESSION.get(url, allow_redirects=True, timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT),
                        headers={"User-Agent": USER_AGENT})
        return (200 <= r.status_code < 400) and ("O produto não existe" not in (r.text or ""))
    except Exception:
        return False

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

def coletar_ofertas(
    partner_id: int,
    api_key: str,
    *,
    keywords: List[str],
    shop_ids: List[int],
    paginas: int,
    itens_por_pagina: int,
) -> List[Dict[str, Any]]:
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
                        "productName": name.strip(),
                        "priceMin": p.get("priceMin"),
                        "priceMax": p.get("priceMax"),
                        "offerLink": p.get("offerLink"),
                        "productLink": p.get("productLink"),
                        "shopName": (p.get("shopName") or "").strip(),
                        "ratingStar": p.get("ratingStar"),
                        "sales": p.get("sales"),
                        "priceDiscountRate": p.get("priceDiscountRate"),
                    }
                )
            time.sleep(2)  # respeitar rate
    dedup = {it["itemId"]: it for it in ofertas}
    return list(dedup.values())

# ===== Filtros por categoria =====
def parse_map_env(env_name: str) -> Dict[str, float]:
    raw = os.getenv(env_name, "").strip()
    result: Dict[str, float] = {}
    if not raw:
        return result
    for part in raw.split(","):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        k = k.strip().lower()
        try:
            result[k] = float(v)
        except Exception:
            continue
    return result

def filter_quality_by_category(products: List[Dict[str, Any]], *, min_rating: float) -> List[Dict[str, Any]]:
    default_min_sales = getenv_int("MIN_SALES_DEFAULT", 100)
    min_sales_map = parse_map_env("MIN_SALES_BY_CAT")
    default_min_disc = getenv_float("MIN_DISCOUNT", 0.15)
    min_disc_map = parse_map_env("MIN_DISCOUNT_BY_CAT")

    out: List[Dict[str, Any]] = []
    for p in products:
        name = p.get("productName") or ""
        cat = tag_categoria(name)
        try:
            rating = float(p.get("ratingStar") or 0.0)
        except Exception:
            rating = 0.0
        try:
            disc = float(p.get("priceDiscountRate") or 0.0)
        except Exception:
            disc = 0.0
        sales_val = p.get("sales")
        sales = int(sales_val) if (isinstance(sales_val, (int, float)) or (isinstance(sales_val, str) and sales_val.isdigit())) else 0

        cat_key = cat.lower()
        min_sales = int(min_sales_map.get(cat_key, default_min_sales))
        min_disc = float(min_disc_map.get(cat_key, default_min_disc))

        # exceção leve para categorias com oferta escassa
        min_rating_eff = min_rating
        if cat_key in ("câmera/segurança", "projetor"):
            min_rating_eff = max(4.6, min_rating - 0.1)

        if rating >= min_rating_eff and disc >= min_disc and sales >= min_sales:
            out.append(p)
    return out


# ==== Config & Helpers de Título/Copy (anti-repetição/sem 'Aproveite' no título) ====
TITLE_MAX_LEN = getenv_int("TITLE_MAX_LEN", 110)  # limite para títulos no Telegram
INCLUDE_APROVEITE_IN_TITLE = getenv_bool("INCLUDE_APROVEITE_IN_TITLE", False)

def compact_name(product_name: str) -> str:
    """Remove ruído e encurta o nome mantendo marca + substantivo principal."""
    s = (product_name or "").strip()
    # remover termos redundantes comuns
    s = re.sub(r"\b(original|novo|gamer|bluetooth|wireless|com fio|sem fio|rgb|led|headset)\b", "", s, flags=re.I)
    # normaliza espaços
    s = re.sub(r"\s{2,}", " ", s).strip()
    # limitar para ~8 palavras para caber no mobile
    parts = s.split()
    if len(parts) > 8:
        s = " ".join(parts[:8])
    return s

def remove_redundancy(copy_txt: str, product_name: str) -> str:
    """Evita repetição do nome do produto e CTAs desnecessárias na copy curta."""
    t = (copy_txt or "").strip()
    pn = (product_name or "").strip()
    if not t:
        return t
    # Se começa com o nome, remove prefixo
    if pn and t.lower().startswith(pn.lower()):
        t = t[len(pn):].lstrip(":–- ").strip()
    # Remove duplicatas simples separadas por pontuação comum
    chunks = [c.strip() for c in re.split(r"[—:\.]", t) if c.strip()]
    dedup = []
    seen = set()
    for c in chunks:
        key = c.lower()
        if key not in seen:
            dedup.append(c)
            seen.add(key)
    t = " — ".join(dedup)
    # remove CTA (será adicionada pelo botão/rodapé do publisher)
    t = re.sub(r"\b(aproveite|garanta (o seu|a sua)|ver oferta|compre agora)\b[\.!\s]*$", "", t, flags=re.I).strip()
    # limpeza final
    t = re.sub(r"\s{2,}", " ", t)
    t = re.sub(r"[!?.]{2,}$", "", t)
    return t

def clip_len(s: str, max_len: int) -> str:
    """Corta sem '...' e sem quebrar palavras, privilegiando legibilidade."""
    if len(s) <= max_len:
        return s
    short = s[:max_len+1].rsplit(" ", 1)[0].rstrip("—,;: ")
    if len(short) < max_len * 0.66:
        short = s[:max_len].strip()
    return short

def make_headline(product_name: str, category: str, ia_text: str) -> str:
    """Título final: emoji + nome compacto + benefício (sem CTA, sem repetição)."""
    emoji = EMOJI_BY_CAT.get(category, "✨")
    base = compact_name(product_name)
    benefit = remove_redundancy(ia_text, product_name)
    if not benefit:
        defaults = {
            "mouse/teclado/periféricos": "precisão e conforto no uso diário",
            "smartwatch/wearables": "medições práticas e visual moderno",
            "caixa de som/speaker": "som equilibrado para músicas e vídeos",
            "projetor": "imagem nítida para filmes e séries",
            "cozinha (airfryer etc.)": "praticidade e menos sujeira",
            "câmera/segurança": "monitoramento simples e confiável",
            "papelaria": "organização e produtividade",
            "outros": "funcional para o dia a dia",
        }
        benefit = defaults.get(category, "funcional para o dia a dia")
    title = f"{emoji} {base} — {benefit}".strip()
    if not INCLUDE_APROVEITE_IN_TITLE:
        title = re.sub(r"\bAproveite\b\.?$", "", title, flags=re.I).strip()
    title = clip_len(title, TITLE_MAX_LEN)
    return title
# ===== Guardrails / Heurísticas de copy =====
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
    (re.compile(r"\bcetim\b", re.IGNORECASE), lambda m: "cetim anti-frizz"),
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

GENERIC_PHRASES = [
    "com ótimo custo-benefício no dia a dia",
    "praticidade para sua rotina",
    "conforto e qualidade no uso diário",
    "funcional e versátil para diferentes usos",
]

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
    if pn and t.lower().startswith(pn.lower()):
        t = t[len(pn):].lstrip(": ").lstrip("- ").strip()

    if len(t) < 40:
        from random import choice
        t = f"{pn}: {choice(GENERIC_PHRASES)}." if pn else f"{choice(GENERIC_PHRASES)}."

    if hint and hint.lower() not in t.lower() and len(t) < 140:
        if ": " in t[:60]:
            t = t.replace(": ", f": {hint} — ", 1)
        else:
            t = f"{t} — {hint}"
    if not URGENCY_TAIL.search(t) and len(t) < 120:
        t = (t + " Aproveite.").strip()
    if len(t) > 170:
        t = t[:165].rsplit(" ", 1)[0] + "..."
    t = re.sub(r"!{2,}", "!", t)
    return t

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
    ev = compute_ev_signal(
        db_path,
        item_id=int(prod.get("itemId") or 0),
        product_name=prod.get("productName", ""),
        shop_name=prod.get("shopName"),
    )
    return 0.45 * disc + 0.35 * rating_n + 0.20 * ev


def heuristic_copies(prod: Dict[str, Any]) -> Dict[str, Any]:
    name = str(prod.get("productName") or "Oferta").strip()
    cat = tag_categoria(name)
    hint = derive_hint(name)

    if "cetim" in name.lower() or "touca" in name.lower() or "gorro" in name.lower():
        a = "menos frizz e fios protegidos durante a noite"
        b = "conforto ao dormir com tecido macio e anti-frizz"
    elif cat == "mouse/teclado/periféricos":
        a = f"precisão e resposta para elevar seu jogo{(' — ' + hint) if hint else ''}"
        b = f"controle rápido e pegada confortável{(' — ' + hint) if hint else ''}"
    elif cat == "caixa de som/speaker":
        a = "som equilibrado e portabilidade para qualquer ambiente"
        b = f"áudio limpo para músicas e vídeos{(' — ' + hint) if hint else ''}"
    elif cat == "cozinha (airfryer etc.)":
        a = "menos sujeira e praticidade no preparo"
        b = "reutilizável e fácil de limpar"
    else:
        a = "praticidade para o dia a dia"
        b = "funcional e versátil"

    a = remove_redundancy(a, name)
    b = remove_redundancy(b, name)
    return {"texto_de_venda_a": a, "texto_de_venda_b": b}

# ===== Deduplicação por assinatura =====
def dedupe_by_signature(candidates: List[Dict[str, Any]], db_path: str) -> List[Dict[str, Any]]:
    scored = [(heuristic_score(p, db_path), p) for p in candidates]
    buckets: Dict[Tuple, Tuple[float, Dict[str, Any]]] = {}
    for score, p in scored:
        key = make_signature_key(p.get("productName") or "")
        if key not in buckets or score > buckets[key][0]:
            buckets[key] = (score, p)
    return [p for _, p in buckets.values()]

# ===== preço mediano 30 dias (fallback direto no DB se Storage não tiver) =====
def below_median_30d(db_path: str, item_id: int, current_price: Optional[float]) -> bool:
    if not db_path or not os.path.exists(db_path) or current_price in (None, 0):
        return False
    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        now = int(time.time())
        since = now - 30*24*3600
        candidates = [
            ("price_points", "price", "ts"),
            ("price_history", "price", "ts"),
            ("historico_precos", "preco", "timestamp"),
        ]
        prices = []
        for table, col_price, col_ts in candidates:
            try:
                cur.execute(f"SELECT {col_price} FROM {table} WHERE item_id=? AND {col_ts}>=? ORDER BY {col_ts} DESC", (item_id, since))
                rows = cur.fetchall()
                if rows:
                    prices = [float(r[0]) for r in rows if r[0] is not None]
                    break
            except Exception:
                continue
        con.close()
        if len(prices) < 4:
            return False
        prices.sort()
        mid = (len(prices)-1)//2
        if len(prices) % 2 == 1:
            median = prices[mid]
        else:
            median = 0.5*(prices[mid] + prices[mid+1])
        return current_price <= median*0.98
    except Exception:
        return False

# ===== A/B por categoria =====
def parse_variant_map(env_name: str) -> Dict[str, str]:
    raw = os.getenv(env_name, "").strip()
    result: Dict[str, str] = {}
    if not raw:
        return result
    for part in raw.split(","):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        k = k.strip().lower()
        result[k] = v.strip().upper()[:1] or "A"
    return result

def pick_variant_for_category(category: str, rnd: random.Random) -> str:
    default_map = parse_variant_map("AB_DEFAULT_VARIANT_BY_CAT")
    explore_pct = float(os.getenv("AB_EXPLORE_PCT", "0.3"))
    cat_key = (category or "outros").lower()
    base = default_map.get(cat_key, "A").upper()
    if rnd.random() < explore_pct:
        return "B" if base == "A" else "A"
    return base

# ===== Helpers de link =====
def add_subid(url: str, sub_id: str) -> str:
    try:
        pr = urlparse(url)
        q = dict(parse_qsl(pr.query, keep_blank_values=True))
        # a Shopee costuma aceitar subId ou utmContent dependendo da geração do link
        if "subId" not in q and "utmContent" not in q:
            q["subId"] = sub_id
        new_q = urlencode(q, doseq=True)
        return urlunparse(pr._replace(query=new_q))
    except Exception:
        return url

def build_title(product_name: str, copy_text: str, category: str) -> str:
    emoji = EMOJI_BY_CAT.get(category, "✨")
    base = f"{emoji} {product_name}: {copy_text}"
    return (base[:200] + "…") if len(base) > 201 else base

def is_below_median_30d_storage(db: Storage, item_id: int, price_now: float|None, db_path: str) -> bool:
    if not price_now:
        return False
    try:
        med = db.median_price_30d(item_id)
        if med is None:
            return below_median_30d(db_path, item_id, price_now)
        return float(price_now) <= 0.98*float(med)
    except Exception:
        return below_median_30d(db_path, item_id, price_now)

# ===== Main =====
def main():
    PARTNER_ID_STR = getenv_required("SHOPEE_PARTNER_ID")
    API_KEY = getenv_required("SHOPEE_API_KEY")
    _ = getenv_required("GEMINI_API_KEY")  # exige esta VAR (mesmo IA_ENABLED=0, para manter consistência)
    TELEGRAM_BOT_TOKEN = getenv_required("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = getenv_required("TELEGRAM_CHAT_ID")
    try:
        PARTNER_ID = int(PARTNER_ID_STR)
    except Exception:
        sys.exit("ERRO: SHOPEE_PARTNER_ID inválido (não numérico).")

    QTD_POSTS = getenv_int("QUANTIDADE_DE_POSTS_POR_EXECUCAO", 4)
    PAGINAS = getenv_int("PAGINAS_A_VERIFICAR", 2)
    ITENS_POR_PAGINA = getenv_int("ITENS_POR_PAGINA", 15)
    MIN_RATING = getenv_float("MIN_RATING", 4.7)
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
    gross = coletar_ofertas(
        PARTNER_ID, API_KEY,
        keywords=keywords, shop_ids=shop_ids,
        paginas=PAGINAS, itens_por_pagina=ITENS_POR_PAGINA,
    )
    logger.info("Coleta bruta: %d ofertas", len(gross))

    # Persistência leve e price tracking
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

    # Filtro de qualidade por categoria
    candidates = filter_quality_by_category(gross, min_rating=MIN_RATING)
    logger.info("Candidatos após filtros de qualidade: %d", len(candidates))
    if not candidates:
        logger.warning("Sem candidatos após filtros. Encerrando.")
        return 0

    # Deduplicação por assinatura
    candidates = dedupe_by_signature(candidates, DB_PATH)
    logger.info("Após dedupe por assinatura: %d", len(candidates))

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
                logger.warning("IA indisponível para o lote %s — usando heurística (%s itens). Erro: %s", (i // IA_BATCH_SIZE) + 1, len(batch), e)
                for p in batch:
                    iid = int(p["itemId"])
                    h = heuristic_copies(p)
                    pre = next((s for s, pp in prelim if pp is p), 0.4)
                    score = int(55 + (82 - 55) * max(0.0, min(1.0, pre)))
                    ia_results.append({"itemId": iid, "pontuacao": score,
                                       "texto_de_venda_a": h["texto_de_venda_a"],
                                       "texto_de_venda_b": h["texto_de_venda_b"]})

    remaining = [p for p in candidates if p not in top_for_ia]
    for p in remaining:
        iid = int(p["itemId"])
        h = heuristic_copies(p)
        pre = heuristic_score(p, DB_PATH)
        score = int(58 + (78 - 58) * max(0.0, min(1.0, pre)))
        ia_results.append({"itemId": iid, "pontuacao": score,
                           "texto_de_venda_a": h["texto_de_venda_a"],
                           "texto_de_venda_b": h["texto_de_venda_b"]})

    ia_by_id = {int(x["itemId"]): x for x in ia_results if str(x.get("itemId", "")).isdigit()}

    # ===== Ranking final e pool de publicação =====
    ranked_tuples: List[Tuple[float, Dict[str, Any], Dict[str, Any]]] = []
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
        ranked_tuples.append((final, ia, p))
    ranked_tuples.sort(key=lambda x: x[0], reverse=True)

    # Aplica caps por categoria + cooldown antes de tentar publicar
    def select_with_caps_and_dedupe(
        ranked: List[Tuple[float, Dict[str, Any], Dict[str, Any]]],
        *, max_posts: int, max_share: float, db: Storage, cooldown_days: int
    ) -> List[Tuple[float, Dict[str, Any], Dict[str, Any]]]:
        cap = max(1, int(max_posts * max_share))
        chosen: List[Tuple[float, Dict[str, Any], Dict[str, Any]]] = []
        cat_counts: Dict[str, int] = {}
        seen_norm: set[str] = set()

        for final, ia_item, prod in ranked:
            if len(chosen) >= max_posts:
                break
            cat = tag_categoria(prod.get("productName") or "")
            norm = norm_name(prod.get("productName") or "")
            item_id = int(prod.get("itemId") or 0)
            if not item_id:
                continue
            if not db.can_repost(item_id, cooldown_days=cooldown_days):
                continue
            if norm in seen_norm:
                continue
            if cat_counts.get(cat, 0) >= cap:
                continue
            chosen.append((final, ia_item, prod))
            seen_norm.add(norm)
            cat_counts[cat] = cat_counts.get(cat, 0) + 1

        if len(chosen) < max_posts:
            for final, ia_item, prod in ranked:
                if len(chosen) >= max_posts:
                    break
                item_id = int(prod.get("itemId") or 0)
                if not item_id or not db.can_repost(item_id, cooldown_days=cooldown_days):
                    continue
                norm = norm_name(prod.get("productName") or "")
                if norm in seen_norm:
                    continue
                chosen.append((final, ia_item, prod))
                seen_norm.add(norm)

        return chosen

    selected = select_with_caps_and_dedupe(ranked_tuples, max_posts=QTD_POSTS, max_share=MAX_CATEGORY_SHARE, db=db, cooldown_days=COOLDOWN_DIAS)
    logger.info("Selecionados (após caps/dedupe): %d", len(selected))

    # Monta o pool principal (produtos enriquecidos com IA e meta)
    publish_pool: List[Dict[str, Any]] = []
    for final, ia_item, prod in selected:
        prod_ext = dict(prod)
        prod_ext["_final_score"] = float(final)
        prod_ext["_ia"] = dict(ia_item)
        publish_pool.append(prod_ext)

    # ===== Funções de publicação/backfill (rescue_publish) =====
    rnd = random.Random()

    def can_repost(item_id: int) -> bool:
        try:
            return db.can_repost(int(item_id), cooldown_days=COOLDOWN_DIAS)
        except Exception:
            return True

    def publish_func(prod: Dict[str, Any]) -> bool:
        """Publica um único produto usando TelegramPublisher (HTML seguro + fallbacks).
        Integra IA (copy), CTA por variante e tracking via subId.
        """
        try:
            iid = int(prod.get("itemId") or 0)
            if not iid:
                return False
            pname = str(prod.get("productName") or "")
            cat = tag_categoria(pname)
            hint = derive_hint(pname)
            ia = prod.get("_ia") or ia_by_id.get(iid) or heuristic_copies(prod)
            text_a = ia.get("texto_de_venda_a")
            text_b = ia.get("texto_de_venda_b")
            variant = pick_variant_for_category(cat, rnd)
            raw_text = text_a if variant == "A" else text_b
            # Título inclui a copy (para compatibilidade com publisher atual)
            title = make_headline(pname, cat, raw_text)

            # Rodapé/CTA/link
            try:
                price = float(prod.get("priceMin") or 0.0)
            except Exception:
                price = 0.0
            shop = str(prod.get("shopName") or "").strip()
            rating = prod.get("ratingStar")
            sales = prod.get("sales")
            offer = str(prod.get("offerLink") or prod.get("productLink") or "")

            sub_id = f"{iid}-{variant}-{time.strftime('%Y%m%d')}"
            link = add_subid(offer, sub_id)
            cta = "Ver oferta" if variant == "A" else "Abrir no app"

            if DRY_RUN:
                logger.info("[DRY RUN][%s] %s | %s | %s", variant, title[:80], shop, link)
                db.record_post(iid, variant=variant, message_id=f"dryrun-{int(time.time())}")
                return True

            ok = pub.send(
                title=title,
                price_brl=price,
                store=shop,
                rating=float(rating) if rating not in (None, "") else None,
                sales=int(sales) if isinstance(sales, (int, float, str)) and str(sales).isdigit() else None,
                link=link,
                cta=cta,
                variant=variant,
                allow_preview=True,
            )
            if ok:
                db.record_post(iid, variant=variant, message_id=str(int(time.time())))  # sem ID de mensagem, registrar timestamp
                logger.info("Publicado [%s] item %s | score=%.2f", variant, iid, float(prod.get("_final_score") or 0.0))
                return True
            return False
        except Exception as e:
            logger.warning("Erro ao publicar item: %s", e)
            return False

    def collect_relaxed() -> List[Dict[str, Any]]:
        """Backfill com filtros relaxados sobre a própria coleta bruta, ordenado por heurística."""
        if not gross:
            return []
        # Quase-sem filtros (mais permissivo)
        relaxed = []
        bl = get_blocklist_patterns()
        for p in gross:
            if is_blocked(p.get("productName",""), bl):
                continue
            try:
                rating = float(p.get("ratingStar") or 0.0)
            except Exception:
                rating = 0.0
            try:
                disc = float(p.get("priceDiscountRate") or 0.0)
            except Exception:
                disc = 0.0
            sales_val = p.get("sales")
            sales = int(sales_val) if (isinstance(sales_val, (int, float)) or (isinstance(sales_val, str) and sales_val.isdigit())) else 0
            if rating >= max(4.5, MIN_RATING - 0.3) and disc >= max(0.05, float(os.getenv("MIN_DISCOUNT", 0.15)) - 0.08) and sales >= max(20, getenv_int("MIN_SALES_DEFAULT",100) // 4):
                relaxed.append(p)
        # Dedupe por assinatura e ordenar por heurística
        relaxed = dedupe_by_signature(relaxed, DB_PATH)
        relaxed_scored = sorted(relaxed, key=lambda p: heuristic_score(p, DB_PATH), reverse=True)
        # enriquecer com cópias heurísticas, pois a IA pode estar indisponível no modo resgate
        pool: List[Dict[str, Any]] = []
        for p in relaxed_scored:
            ext = dict(p)
            ext["_final_score"] = heuristic_score(p, DB_PATH)
            ext["_ia"] = heuristic_copies(p)
            pool.append(ext)
        return pool

    # Publicação com RESGATE
    posted, tried = publish_with_rescue(
        ranked=publish_pool,
        max_posts=QTD_POSTS,
        can_repost=can_repost,
        publish_func=publish_func,
        collect_relaxed=collect_relaxed,
        id_key="itemId",
        sleep_between=0.6,
    )
    logger.info("Publicações concluídas: %d (tentativas: %d)", posted, tried)
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