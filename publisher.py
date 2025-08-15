
from __future__ import annotations

import random
from html import escape
from typing import Optional
import requests

TELEGRAM_API = "https://api.telegram.org"

def with_utm(link: str, campaign: str, sub_id: Optional[str] = None) -> str:
    from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl
    u = urlparse(link); q = dict(parse_qsl(u.query))
    q.update({"utm_source": "telegram", "utm_medium": "bot", "utm_campaign": campaign})
    if sub_id: q["sub_id"] = sub_id
    u = u._replace(query=urlencode(q)); return urlunparse(u)

def _split_headline(text: str) -> tuple[str, str]:
    """Return (headline_sentence, remainder)."""
    s = (text or "").strip()
    if not s:
        return "", ""
    # first sentence up to period or up to ~140 chars
    i = s.find(". ")
    if 0 < i < 140:
        return s[:i+1], s[i+2:]
    # fallback: cut around 140 chars
    if len(s) > 140:
        cut = s[:140]
        j = cut.rfind(" ")
        if j > 0:
            return cut[:j] + "...", s[j+1:]
    return s, ""

def _fmt_currency_br(v: float) -> str:
    s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return s

def _fmt_sales_br(sales: Optional[int]) -> str:
    if sales is None:
        return ""
    try:
        s = int(sales)
    except Exception:
        return ""
    if s <= 0:
        return ""
    if s >= 1000:
        k = s / 1000.0
        k_str = f"{k:.1f}".rstrip("0").rstrip(".").replace(".", ",")
        return f" • {k_str} mil+ vendidos"
    return f" • {s}+ vendidos"

class TelegramPublisher:
    def __init__(self, bot_token: str, chat_id: str, rate_limit_per_sec: float = 1.0):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.session = requests.Session()
        self._rate_tokens = 5
        self._rate_per_sec = rate_limit_per_sec
        self._last_ts = 0.0

    def _consume_rate(self):
        import time
        now = time.time()
        delta = now - self._last_ts
        self._rate_tokens = min(5, self._rate_tokens + delta * self._rate_per_sec)
        self._last_ts = now
        if self._rate_tokens < 1:
            sleep_for = (1 - self._rate_tokens) / self._rate_per_sec
            time.sleep(max(0.0, sleep_for))
            self._rate_tokens = 0.0
        else:
            self._rate_tokens -= 1

    def send_message(self, text: str, disable_web_page_preview: bool = False) -> Optional[int]:
        self._consume_rate()
        url = f"{TELEGRAM_API}/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": disable_web_page_preview}
        r = self.session.post(url, json=payload, timeout=(8, 20))
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            return None
        return int(data["result"]["message_id"])

    def build_message(
        self,
        *,
        texto_ia: str,
        price: float,
        shop: str,
        offer: str,
        rating: Optional[float] = None,
        discount_rate: Optional[float] = None,
        sales: Optional[int] = None,
        badge: Optional[str] = None,
        campaign: str = "",
        sub_id: Optional[str] = None,
    ) -> str:
        headline, rest = _split_headline(texto_ia)
        offer_url = with_utm(offer, campaign=campaign, sub_id=sub_id)

        # Price line (prefer "de/por" when discount_rate is valid)
        preco_fmt = _fmt_currency_br(price)
        price_line = f"<b>Preço:</b> R$ {preco_fmt}"
        try:
            dr = float(discount_rate) if discount_rate is not None else None
        except Exception:
            dr = None
        if dr is not None and 0.0 < dr < 1.0:
            try:
                de = price / (1.0 - dr)
                de_fmt = _fmt_currency_br(de)
                price_line = f"<b>Preço:</b> <s>R$ {de_fmt}</s> R$ {preco_fmt}"
            except Exception:
                pass

        stars = f"\n⭐ <b>{rating:.1f}+</b>" if rating is not None else ""
        sales_txt = _fmt_sales_br(sales)
        badge_line = f" • {escape(badge)}" if badge else ""

        link_label = random.choice(["Ver oferta", "Comprar com desconto", "Ir à oferta", "Aproveitar oferta"])

        msg = (
            f"<b>{escape(headline)}</b> {escape(rest)}\n\n"
            f"{price_line}\n"
            f"<b>Loja:</b> {escape(shop)}{stars}{sales_txt}{badge_line}\n"
            f"<a href=\"{escape(offer_url)}\"><b>{link_label}</b></a>"
        )
        return msg