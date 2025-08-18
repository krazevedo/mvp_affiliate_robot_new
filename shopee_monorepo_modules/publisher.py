# publisher.py — envio robusto ao Telegram (HTML seguro + fallbacks)
from __future__ import annotations
import requests, html, time, logging
from typing import Optional, Dict, Any

log = logging.getLogger("publisher")

def _escape_html_text(s: str) -> str:
    # escapa texto; não use para URLs
    return html.escape(s, quote=True)

def _safe_url(url: str) -> str:
    # Telegram aceita & sem precisar virar &amp; no atributo href
    return url.strip()

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
            log.error("Telegram erro %s: %s", r.status_code, desc)
        r.raise_for_status()
        return j

    def send(self, title: str, price_brl: float, store: str, rating: Optional[float], sales: Optional[int],
             link: str, cta: str, variant: str, allow_preview: bool = True) -> bool:
        """Tenta enviar com HTML; se der 400, reenviar sem parse_mode e/ou dividido."""
        # Monta mensagem
        t = _escape_html_text(title)
        s = _escape_html_text(store)
        cta_txt = _escape_html_text(cta)
        price = f"R$ {price_brl:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        meta = []
        if rating is not None:
            meta.append(f"⭐️ {rating:.1f}+")
        if sales is not None:
            meta.append(f"{sales}+ vendidos")
        meta_line = " • ".join(meta) if meta else ""

        url = _safe_url(link)
        msg_html = f"<b>{t}</b>\n\nPreço: <b>{price}</b>\nLoja: {s}\n{meta_line}\n\n<a href=\"{url}\">{cta_txt}</a> • Variante: {variant}"

        payload = {
            "chat_id": self.chat_id,
            "text": msg_html[:3900],
            "parse_mode": "HTML",
            "disable_web_page_preview": (not allow_preview)
        }
        try:
            self._send(payload)
            return True
        except requests.HTTPError as e:
            # Fallback 1: enviar sem parse_mode (texto puro com link em linha separada)
            log.warning("HTML falhou, tentando texto puro. Motivo: %s", str(e))
            plain = f"{title}\n\nPreço: {price}\nLoja: {store}\n{meta_line}\n\n{cta}:\n{url}\nVariante: {variant}"
            payload2 = {
                "chat_id": self.chat_id,
                "text": plain[:3900],
                "disable_web_page_preview": (not allow_preview)
            }
            try:
                self._send(payload2)
                return True
            except requests.HTTPError as e2:
                log.error("Falha também no texto puro: %s", str(e2))
                # Fallback 2: mensagem mínima (reduzir risco de parse/limites)
                minimal = f"{title} — {price}\n{url}"
                payload3 = {"chat_id": self.chat_id, "text": minimal[:3800], "disable_web_page_preview": True}
                try:
                    self._send(payload3)
                    return True
                except requests.HTTPError as e3:
                    log.error("Falha no fallback mínimo: %s", str(e3))
                    return False
