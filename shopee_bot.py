#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
shopee_bot.py
Coleta ofertas via GraphQL do programa de afiliados da Shopee, filtra, ranqueia (IA opcional),
e publica no Telegram com fallback, garantindo volume mesmo com cooldown.

Requisitos:
- requests
- sqlite3 (std lib)
- (opcional) ai.py com analyze_products / IAResponse / IAItem

Ambiente (ENV):
  SHOPEE_PARTNER_ID, SHOPEE_API_KEY
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  DB_PATH= data/bot.db
  QUANTIDADE_DE_POSTS_POR_EXECUCAO=6
  PAGINAS_A_VERIFICAR=2
  ITENS_POR_PAGINA=15
  MIN_RATING=4.7
  MIN_DISCOUNT=0.15
  MIN_IA_SCORE=65
  COOLDOWN_REPOSTAGEM_DIAS=5
  MAX_CATEGORY_SHARE=0.5
  IA_ENABLED=1
  IA_TOP_K=6
  IA_BATCH_SIZE=6
  AB_VARIANT=A
  CTA_DEFAULT=Ver oferta
  DRY_RUN=0
"""

from __future__ import annotations

import os
import re
import json
import time
import math
import html
import uuid
import queue
import sqlite3
import logging
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple

import requests

# ===========================
# Logging
# ===========================
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("shopee_bot")

# ===========================
# Config
# ===========================
PARTNER_ID = os.getenv("SHOPEE_PARTNER_ID", "").strip()
API_KEY = os.getenv("SHOPEE_API_KEY", "").strip()
GRAPHQL_URL = "https://open-api.affiliate.shopee.com.br/graphql"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

DB_PATH = os.getenv("DB_PATH", "data/bot.db")

QTY_POSTS = int(os.getenv("QUANTIDADE_DE_POSTS_POR_EXECUCAO", "6"))
PAGINAS_A_VERIFICAR = int(os.getenv("PAGINAS_A_VERIFICAR", "2"))
ITENS_POR_PAGINA = int(os.getenv("ITENS_POR_PAGINA", "15"))

MIN_RATING = float(os.getenv("MIN_RATING", "4.7"))
MIN_DISCOUNT = float(os.getenv("MIN_DISCOUNT", "0.15"))  # 15%
MIN_IA_SCORE = float(os.getenv("MIN_IA_SCORE", "65"))

COOLDOWN_DIAS = int(os.getenv("COOLDOWN_REPOSTAGEM_DIAS", "5"))
MAX_CATEGORY_SHARE = float(os.getenv("MAX_CATEGORY_SHARE", "0.5"))  # cap de diversidade por categoria

IA_ENABLED = os.getenv("IA_ENABLED", "1") == "1"
IA_TOP_K = int(os.getenv("IA_TOP_K", "6"))
IA_BATCH_SIZE = int(os.getenv("IA_BATCH_SIZE", "6"))

AB_VARIANT = os.getenv("AB_VARIANT", "A").strip().upper() or "A"
CTA_DEFAULT = os.getenv("CTA_DEFAULT", "Ver oferta")

DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

KEYWORDS = [
    "caixa de som bluetooth",
    "fone de ouvido sem fio",
    "smartwatch",
    "teclado mecanico",
    "mouse gamer",
    "air fryer",
    "projetor hy300",
    "camera de segurança",
]

SHOP_IDS = [369632653, 288420684, 286277644, 1157280425, 1315886500, 349591196, 886950101]

# ===========================
# DB Helpers
# ===========================
def db_connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) if os.path.dirname(DB_PATH) else None
    con = sqlite3.connect(DB_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER NOT NULL,
        title TEXT,
        category TEXT,
        variant TEXT,
        cta_used TEXT,
        posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    con.commit()
    return con

def register_post(con: sqlite3.Connection, item_id: int, title: str, category: Optional[str], variant: str, cta: str) -> None:
    con.execute("INSERT INTO posts (item_id, title, category, variant, cta_used) VALUES (?,?,?,?,?)",
                (item_id, title, category, variant, cta))
    con.commit()

def last_posted_ts(con: sqlite3.Connection, item_id: int) -> Optional[float]:
    row = con.execute("SELECT strftime('%s', posted_at) FROM posts WHERE item_id=? ORDER BY posted_at DESC LIMIT 1",
                      (item_id,)).fetchone()
    if row and row[0]:
        try:
            return float(row[0])
        except Exception:
            return None
    return None

def can_repost(con: sqlite3.Connection, item_id: int, cooldown_days: int) -> bool:
    ts = last_posted_ts(con, item_id)
    if not ts:
        return True
    delta_days = (time.time() - ts) / 86400.0
    if delta_days >= cooldown_days:
        return True
    logger.info("Cooldown ativo para item %s — pulando", item_id)
    return False

# ===========================
# Telegram Publisher (robusto)
# ===========================
try:
    from shopee_monorepo_modules.publisher import TelegramPublisher  # se existir módulo separado
except Exception:
    class TelegramPublisher:
        def __init__(self, bot_token: str, chat_id: str, timeout: int = 15):
            self.base = f"https://api.telegram.org/bot{bot_token}"
            self.chat_id = chat_id
            self.timeout = timeout

        def _send(self, payload: Dict[str, Any]) -> Dict[str, Any]:
            url = f"{self.base}/sendMessage"
            r = requests.post(url, json=payload, timeout=self.timeout)
            try:
                j = r.json()
            except Exception:
                j = {}
            if r.status_code != 200:
                desc = j.get("description") or r.text
                logger.error("Telegram erro %s: %s", r.status_code, desc)
            r.raise_for_status()
            return j

        def send(self, title: str, price_brl: float, store: str, rating: Optional[float], sales: Optional[int],
                 link: str, cta: str, variant: str, allow_preview: bool = True) -> bool:
            if DRY_RUN:
                logger.info("[DRY_RUN] %s — %s — %s", title, f"R$ {price_brl:.2f}", link)
                return True

            t = html.escape(title, quote=True)
            s = html.escape(store, quote=True)
            cta_txt = html.escape(cta, quote=True)
            price = f"R$ {price_brl:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            meta = []
            if rating is not None:
                meta.append(f"⭐️ {rating:.1f}+")
            if sales is not None:
                meta.append(f"{sales}+ vendidos")
            meta_line = " • ".join(meta) if meta else ""

            url = link.strip()
            msg_html = f"<b>{t}</b>\n\nPreço: <b>{price}</b>\nLoja: {s}\n{meta_line}\n\n<a href=\"{url}\">{cta_txt}</a> • Variante: {variant}"
            payload = {
                "chat_id": self.chat_id,
                "text": msg_html[:3900],
                "parse_mode": "HTML",
                "disable_web_page_preview": (not allow_preview),
            }
            try:
                self._send(payload)
                return True
            except requests.HTTPError as e:
                logger.warning("HTML falhou, tentando texto puro. Motivo: %s", str(e))
                plain = f"{title}\n\nPreço: {price}\nLoja: {store}\n{meta_line}\n\n{cta}:\n{url}\nVariante: {variant}"
                payload2 = {
                    "chat_id": self.chat_id,
                    "text": plain[:3900],
                    "disable_web_page_preview": (not allow_preview),
                }
                try:
                    self._send(payload2)
                    return True
                except requests.HTTPError as e2:
                    logger.error("Falha também no texto puro: %s", str(e2))
                    minimal = f"{title} — {price}\n{url}"
                    payload3 = {"chat_id": self.chat_id, "text": minimal[:3800], "disable_web_page_preview": True}
                    try:
                        self._send(payload3)
                        return True
                    except requests.HTTPError as e3:
                        logger.error("Falha no fallback mínimo: %s", str(e3))
                        return False

# ===========================
# Rescue Publisher (backfill)
# ===========================
try:
    from rescue_publish import publish_with_rescue  # se existir módulo separado
except Exception:
    from typing import Callable, Set

    def publish_with_rescue(
        ranked: List[Dict[str, Any]],
        max_posts: int,
        can_repost: Callable[[int], bool],
        publish_func: Callable[[Dict[str, Any]], bool],
        collect_relaxed: Optional[Callable[[], List[Dict[str, Any]]]] = None,
        id_key: str = "item_id",
        sleep_between: float = 0.6,
    ) -> Tuple[int, int]:
        posted = 0
        tried = 0
        seen: Set[int] = set()
        idx = 0

        def _pick_next(pool: List[Dict[str, Any]], start_idx: int) -> int:
            i = start_idx
            while i < len(pool):
                pid = int(pool[i].get(id_key) or 0)
                if pid and pid not in seen and can_repost(pid):
                    return i
                i += 1
            return -1

        # Passo 1: ranking principal
        while posted < max_posts:
            nxt = _pick_next(ranked, idx)
            if nxt == -1:
                break
            prod = ranked[nxt]
            pid = int(prod.get(id_key))
            seen.add(pid)
            tried += 1
            if publish_func(prod):
                posted += 1
                time.sleep(sleep_between)
            idx = nxt + 1

        # Passo 2: backfill no restante
        i = idx
        while posted < max_posts and i < len(ranked):
            pid = int(ranked[i].get(id_key) or 0)
            if pid and pid not in seen and can_repost(pid):
                tried += 1
                if publish_func(ranked[i]):
                    posted += 1
                    time.sleep(sleep_between)
                seen.add(pid)
            i += 1

        # Passo 3: segundo passe relaxado
        if posted < max_posts and collect_relaxed:
            logger.warning("Ativando modo RESGATE: coletando mais itens com filtros relaxados...")
            extra = collect_relaxed() or []
            j = 0
            while posted < max_posts and j < len(extra):
                pid = int(extra[j].get(id_key) or 0)
                if pid and pid not in seen and can_repost(pid):
                    tried += 1
                    if publish_func(extra[j]):
                        posted += 1
                        time.sleep(sleep_between)
                    seen.add(pid)
                j += 1

        return posted, tried

# ===========================
# Shopee GraphQL helpers
# ===========================
HEADERS = {
    "Content-Type": "application/json",
    # Algumas integrações usam PartnerId/APIKey, outras X-Partner-Id/X-API-Key
    "PartnerId": PARTNER_ID,
    "APIKey": API_KEY,
    "X-Partner-Id": PARTNER_ID,
    "X-API-Key": API_KEY,
}

def gql(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"query": query, "variables": variables}
    r = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data["data"]

# Exemplo de consulta: search por palavra-chave
GQL_SEARCH = """
query Search($keyword:String!, $offset:Int!, $limit:Int!) {
  itemSearch(keyword: $keyword, offset: $offset, limit: $limit) {
    totalCount
    items {
      itemId
      name
      price
      priceMin
      priceMax
      discount
      historicalSold
      shopId
      shopName
      rating
      image
      category
      url
    }
  }
}
"""

# Exemplo de consulta por loja
GQL_BY_SHOP = """
query ShopItems($shopId:Long!, $offset:Int!, $limit:Int!) {
  itemSearchByShop(shopId:$shopId, offset:$offset, limit:$limit) {
    totalCount
    items {
      itemId
      name
      price
      priceMin
      priceMax
      discount
      historicalSold
      shopId
      shopName
      rating
      image
      category
      url
    }
  }
}
"""

def collect_items() -> List[Dict[str, Any]]:
    logger.info("Coletando ofertas (GraphQL Affiliate)...")
    all_items: List[Dict[str, Any]] = []

    for kw in KEYWORDS:
        logger.info("Buscando keyword='%s' ...", kw)
        for page in range(PAGINAS_A_VERIFICAR):
            offset = page * ITENS_POR_PAGINA
            try:
                data = gql(GQL_SEARCH, {"keyword": kw, "offset": offset, "limit": ITENS_POR_PAGINA})
                block = (data.get("itemSearch") or {})
                items = block.get("items") or []
                for it in items:
                    all_items.append(normalize_item(it, source="kw", keyword=kw))
            except Exception as e:
                logger.warning("Falha na busca por keyword '%s' (p%d): %s", kw, page, e)

    for sid in SHOP_IDS:
        logger.info("Buscando shopId=%s ...", sid)
        for page in range(PAGINAS_A_VERIFICAR):
            offset = page * ITENS_POR_PAGINA
            try:
                data = gql(GQL_BY_SHOP, {"shopId": int(sid), "offset": offset, "limit": ITENS_POR_PAGINA})
                block = (data.get("itemSearchByShop") or {})
                items = block.get("items") or []
                for it in items:
                    all_items.append(normalize_item(it, source="shop", shopId=sid))
            except Exception as e:
                logger.warning("Falha na busca por shop %s (p%d): %s", sid, page, e)

    logger.info("Coleta bruta: %d ofertas", len(all_items))
    return all_items

def normalize_item(it: Dict[str, Any], **meta) -> Dict[str, Any]:
    """Normaliza campos esperados e monta affiliate_url (com utmContent definido depois)."""
    price = it.get("price") or it.get("priceMin") or it.get("priceMax") or 0
    try:
        price = float(price)
    except Exception:
        price = 0.0
    discount = it.get("discount") or 0
    try:
        discount = float(discount)
        # Alguns retornam 10.0 para 10%; outros 0.10 → vamos padronizar em fração (0-1)
        discount = discount/100.0 if discount > 1.0 else discount
    except Exception:
        discount = 0.0

    item = {
        "item_id": int(it.get("itemId") or 0),
        "name": it.get("name") or "",
        "price": price,
        "discount": discount,
        "historicalSold": int(it.get("historicalSold") or 0),
        "shopId": int(it.get("shopId") or 0),
        "shopName": it.get("shopName") or "",
        "rating": float(it.get("rating") or 0.0),
        "image": it.get("image"),
        "category": it.get("category") or "",
        "url": it.get("url") or "",
        "source_meta": meta,
    }
    return item

# ===========================
# Heurísticas / IA
# ===========================
def dedupe_signature(p: Dict[str, Any]) -> str:
    base = (p.get("name") or "").strip().lower()
    shop = (p.get("shopName") or "").strip().lower()
    price_bucket = int((p.get("price") or 0) // 5)  # agrupa por ~R$5
    return f"{base}|{shop}|{price_bucket}"

def basic_filters(items: List[Dict[str, Any]], min_rating: float, min_discount: float) -> List[Dict[str, Any]]:
    out = []
    for p in items:
        if p["item_id"] <= 0:
            continue
        if p["rating"] and p["rating"] < min_rating:
            continue
        if p["discount"] < min_discount:
            continue
        # pode adicionar checks de palavras proibidas, etc.
        out.append(p)
    return out

def rank_heuristic(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def score(p: Dict[str, Any]) -> float:
        r = float(p.get("rating") or 0)
        d = float(p.get("discount") or 0)
        s = int(p.get("historicalSold") or 0)
        # simples: pondera rating, desconto e popularidade
        return (r * 10.0) + (d * 100.0) + (math.log1p(max(s, 0)) * 5.0)
    return sorted(items, key=score, reverse=True)

# Tenta usar IA se houver ai.py
def analyze_with_ai(items: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    if not IA_ENABLED:
        raise RuntimeError("IA desabilitada")
    try:
        from ai import analyze_products, IAResponse  # type: ignore
    except Exception as e:
        raise RuntimeError(f"IA indisponível: {e}")

    # Monta payload leve para IA
    batch = []
    for p in items[:min(len(items), 30)]:  # evita prompts enormes
        batch.append({
            "item_id": p["item_id"],
            "title": p["name"],
            "price": p["price"],
            "discount": p["discount"],
            "rating": p["rating"],
            "sales": p["historicalSold"],
            "shop": p["shopName"],
            "category": p["category"],
        })
    resp = analyze_products(batch)
    picked_ids = set()
    enriched = []
    for it in resp.items[:top_k]:
        picked_ids.add(int(it.item_id))
    for p in items:
        if p["item_id"] in picked_ids:
            enriched.append(p)
    return enriched

# ===========================
# Diversidade por categoria
# ===========================
def cap_by_category(ranked: List[Dict[str, Any]], limit_share: float, max_total: int) -> List[Dict[str, Any]]:
    if not ranked:
        return []
    # no mínimo 1 por categoria; máximo proporcional
    counts: Dict[str, int] = {}
    total_allowed = max_total
    out: List[Dict[str, Any]] = []
    for p in ranked:
        cat = p.get("category") or "OUTROS"
        limit = max(1, int(total_allowed * limit_share))
        if counts.get(cat, 0) < limit:
            out.append(p)
            counts[cat] = counts.get(cat, 0) + 1
        if len(out) >= total_allowed:
            break
    return out

# ===========================
# Links / UTM (AB variant)
# ===========================
def make_affiliate_url(base_url: str, sub_id: str) -> str:
    if not base_url:
        return ""
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}utm_content={sub_id}"

def make_sub_id(variant: str, item_id: int) -> str:
    # sub-id curtinho: BOT-<var>-<id>
    return f"BOT-{variant}-{item_id}"

# ===========================
# Segundo passe relaxado
# ===========================
def second_pass_relaxed(all_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # reduz limiares para dar volume em dias fracos
    relaxed = basic_filters(all_items, min_rating=max(0.0, MIN_RATING - 0.2), min_discount=max(0.0, MIN_DISCOUNT - 0.05))
    relaxed = rank_heuristic(relaxed)
    return relaxed

# ===========================
# MAIN
# ===========================
def main():
    if not PARTNER_ID or not API_KEY:
        raise SystemExit("Faltam credenciais da Shopee (SHOPEE_PARTNER_ID / SHOPEE_API_KEY)")

    con = db_connect()

    # 1) Coleta
    items = collect_items()

    # 2) Filtros de qualidade
    filtered = basic_filters(items, MIN_RATING, MIN_DISCOUNT)
    logger.info("Candidatos após filtros de qualidade: %d", len(filtered))

    # 3) Dedupe por assinatura
    seen_sig: set[str] = set()
    deduped: List[Dict[str, Any]] = []
    for p in filtered:
        sig = dedupe_signature(p)
        if sig in seen_sig:
            continue
        seen_sig.add(sig)
        deduped.append(p)
    logger.info("Após dedupe por assinatura: %d", len(deduped))

    # 4) Rank (IA opcional)
    ranked: List[Dict[str, Any]] = []
    used_ai = False
    if IA_ENABLED and deduped:
        try:
            ranked_ai = analyze_with_ai(deduped, top_k=max(QTY_POSTS * 2, IA_TOP_K))
            if ranked_ai:
                used_ai = True
                ranked = rank_heuristic(ranked_ai)  # ainda ordena por heurística como desempate
        except Exception as e:
            logger.error("IA indisponível — usando heurística (%d itens). Erro: %s", len(deduped), e)

    if not ranked:
        ranked = rank_heuristic(deduped)

    # 5) Diversidade por categoria e corte
    ranked = cap_by_category(ranked, MAX_CATEGORY_SHARE, max_total=max(QTY_POSTS * 3, QTY_POSTS))
    logger.info("Selecionados (após caps/dedupe): %d", len(ranked))

    # 6) Preparação de links/AB
    for p in ranked:
        sub_id = make_sub_id(AB_VARIANT, p["item_id"])
        p["affiliate_url"] = make_affiliate_url(p["url"], sub_id)
        p["variant"] = AB_VARIANT
        p["cta"] = CTA_DEFAULT

    # 7) Publicação resiliente com backfill e segundo passe
    tp = TelegramPublisher(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

    def can_repost_wrapper(item_id: int) -> bool:
        return can_repost(con, item_id, COOLDOWN_DIAS)

    def publish_func(prod: Dict[str, Any]) -> bool:
        try:
            title = prod.get("name") or "Oferta"
            price = float(prod.get("price") or 0.0)
            store = prod.get("shopName") or "-"
            rating = float(prod["rating"]) if prod.get("rating") is not None else None
            sales = int(prod["historicalSold"]) if prod.get("historicalSold") is not None else None
            link = prod.get("affiliate_url") or prod.get("url") or ""
            cta = prod.get("cta") or CTA_DEFAULT
            variant = prod.get("variant") or AB_VARIANT

            ok = tp.send(
                title=title, price_brl=price, store=store,
                rating=rating, sales=sales, link=link,
                cta=cta, variant=variant, allow_preview=True
            )
            if ok and not DRY_RUN:
                register_post(con, item_id=int(prod["item_id"]), title=title, category=prod.get("category"), variant=variant, cta=cta)
            return ok
        except Exception as e:
            logger.exception("Falha ao publicar item_id=%s: %s", prod.get("item_id"), e)
            return False

    def collect_relaxed_second_pass() -> List[Dict[str, Any]]:
        extra = second_pass_relaxed(items)
        # Prepara links/variant também nos extras
        out = []
        for p in extra:
            sub_id = make_sub_id(AB_VARIANT, p["item_id"])
            p = dict(p)
            p["affiliate_url"] = make_affiliate_url(p["url"], sub_id)
            p["variant"] = AB_VARIANT
            p["cta"] = CTA_DEFAULT
            out.append(p)
        return out

    publicados, tentativas = publish_with_rescue(
        ranked=ranked,
        max_posts=QTY_POSTS,
        can_repost=can_repost_wrapper,
        publish_func=publish_func,
        collect_relaxed=collect_relaxed_second_pass,
        id_key="item_id",
        sleep_between=0.6
    )

    logger.info("Publicações concluídas: %d (tentativas: %d)", publicados, tentativas)


if __name__ == "__main__":
    main()