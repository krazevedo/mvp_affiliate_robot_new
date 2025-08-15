
from __future__ import annotations
import threading, time
from html import escape
from typing import Optional
import requests

TELEGRAM_API = "https://api.telegram.org"

class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self.capacity = burst; self.tokens = burst; self.rate = rate_per_sec; self.last = time.time()
        self.lock = threading.Lock()
    def consume(self, tokens: int = 1) -> None:
        with self.lock:
            now = time.time(); delta = now - self.last
            self.tokens = min(self.capacity, self.tokens + delta * self.rate); self.last = now
            if self.tokens < tokens:
                sleep_for = (tokens - self.tokens) / self.rate; time.sleep(max(0.0, sleep_for)); self.tokens = max(0.0, self.tokens - tokens)
            else:
                self.tokens -= tokens

def with_utm(link: str, campaign: str, sub_id: Optional[str] = None) -> str:
    from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl
    u = urlparse(link); q = dict(parse_qsl(u.query))
    q.update({ "utm_source":"telegram", "utm_medium":"bot", "utm_campaign":campaign })
    if sub_id: q["sub_id"] = sub_id
    u = u._replace(query=urlencode(q)); return urlunparse(u)

class TelegramPublisher:
    def __init__(self, bot_token: str, chat_id: str, rate_limit_per_sec: float = 1.0):
        self.bot_token = bot_token; self.chat_id = chat_id
        self.rate = TokenBucket(rate_limit_per_sec, burst=5); self.session = requests.Session()
    def send_message(self, text: str, disable_web_page_preview: bool = False) -> Optional[int]:
        self.rate.consume()
        url = f"{TELEGRAM_API}/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": disable_web_page_preview}
        r = self.session.post(url, json=payload, timeout=(8,20)); r.raise_for_status(); data = r.json()
        if not data.get("ok"): return None
        return int(data["result"]["message_id"])
    def build_message(self, *, texto_ia: str, price: float, shop: str, offer: str,
                      rating: Optional[float] = None, badge: Optional[str] = None,
                      campaign: str = "", sub_id: Optional[str] = None) -> str:
        safe_ia = escape(texto_ia or "").strip(); safe_shop = escape(shop or "").strip()
        rating_line = f"\n⭐ <b>{rating:.1f}+</b>" if rating else ""
        badge_line = f" • {escape(badge)}" if badge else ""
        offer_url = with_utm(offer, campaign=campaign, sub_id=sub_id)
        preco = f"{price:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return (f"{safe_ia}\n\n<b>Preço:</b> R$ {preco}\n<b>Loja:</b> {safe_shop}{rating_line}{badge_line}\n"
                f"<a href=\"{escape(offer_url)}\"><b>Ver oferta</b></a>")
