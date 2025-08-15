
from __future__ import annotations
import os, time, json, html, logging, requests
from typing import Optional, Dict, Any

logger = logging.getLogger("publisher")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TELEGRAM_API = "https://api.telegram.org"

def _fmt_price_br(v: float) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "—"

def _limit_len(s: str, max_chars: int = 3800) -> str:
    s = s or ""
    if len(s) <= max_chars:
        return s
    return s[:max_chars-3].rsplit(" ", 1)[0] + "..."

def _escape_html(s: str) -> str:
    return html.escape(s or "", quote=False)

class TelegramPublisher:
    def __init__(self, *, bot_token: str, chat_id: str|int, parse_mode: str = "HTML"):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.parse_mode = parse_mode
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "OfferBot/1.5 (+telegram)"})
        self.timeout = (8, 20)

        # CTA variants
        self.cta_variants = {
            "A": "🔗 Ver oferta",
            "B": "🔗 Abrir no app",
        }

    def build_message(
        self,
        *,
        texto_ia: str,
        price: float|None,
        shop: str|None,
        offer: str,
        rating: float|None,
        discount_rate: float|None,
        sales: int|None,
        badge: str|None,
        campaign: str,
        sub_id: str,
        variant: str|None = None,
        abaixo_mediana_30d: bool|None = None,
        popular: bool|None = None,
        desconto_forte: bool|None = None,
        emoji: str|None = None,
        title: str|None = None,
    ) -> Dict[str, Any]:
        # Acrescenta sub_id na oferta
        link = offer or ""
        sep = "&" if "?" in link else "?"
        link = f"{link}{sep}sub_id={_escape_html(sub_id)}"

        # Monta linhas do rodapé
        linhas = []
        if price:
            linhas.append(f"Preço: {_escape_html(_fmt_price_br(price))}")
        if shop:
            linhas.append(f"Loja: {_escape_html(shop)}")
        star_txt = []
        if rating:
            star_txt.append(f"⭐ {rating:.1f}+")
        if sales and sales > 0:
            star_txt.append(f"{sales:+d}".replace("+", "") + "+ vendidos")
        if star_txt:
            linhas.append(" • ".join(star_txt))
        if rating and rating >= 4.8 and sales and sales >= 100:
            linhas.append("Loja bem avaliada")

        # Motivo agora (apenas 1 linha)
        motivo = None
        if abaixo_mediana_30d:
            motivo = "Preço DESPENCOU."
        elif desconto_forte:
            motivo = "SOMENTE Hoje."
        elif popular:
            motivo = "Todo mundo comprando."

        # Título + corpo (HTML)
        texto_ia = (texto_ia or "").strip()
        # Garante uma linha de título em negrito se for passado separado
        if title:
            header = f"<b>{_escape_html(title.strip())}</b>"
        else:
            # tenta extrair a primeira parte antes de dois pontos como título
            if ":" in texto_ia[:80]:
                t, rest = texto_ia.split(":", 1)
                header = f"<b>{_escape_html(t.strip())}</b>\n{_escape_html(rest.strip())}"
                texto_ia = ""
            else:
                header = ""

        corpo = _escape_html(texto_ia) if texto_ia else None

        blocos = []
        if header:
            blocos.append(header)
        if corpo:
            blocos.append(corpo)
        if motivo:
            blocos.append(motivo)

        footer = "\n".join([l for l in linhas if l])
        if footer:
            blocos.append(footer)

        # CTA
        v = (variant or "A").upper()
        cta_label = self.cta_variants.get(v, self.cta_variants["A"])
        blocos.append(f'{cta_label}\n{link}')

        msg = "\n\n".join([b for b in blocos if b])
        msg = _limit_len(msg, 3800)

        return {
            "text": msg,
            "parse_mode": self.parse_mode,
            "disable_web_page_preview": False,
        }

    def send_message(self, payload: Dict[str, Any]) -> Optional[int]:
        url = f"{TELEGRAM_API}/bot{self.bot_token}/sendMessage"
        data = {
            "chat_id": self.chat_id,
            "text": payload["text"],
            "parse_mode": payload.get("parse_mode", "HTML"),
            "disable_web_page_preview": payload.get("disable_web_page_preview", True),
        }

        # Primeira tentativa (HTML)
        try:
            r = self.session.post(url, data=data, timeout=self.timeout)
            if r.status_code == 200:
                j = r.json()
                return j.get("result", {}).get("message_id")
            # Se der parse error, tenta sem parse_mode
            desc = ""
            try:
                desc = r.json().get("description", "")
            except Exception:
                desc = r.text
            if "can't parse entities" in (desc or "").lower():
                logger.warning("Telegram parse error — reenviando sem parse_mode.")
                data.pop("parse_mode", None)
                r2 = self.session.post(url, data=data, timeout=self.timeout)
                if r2.status_code == 200:
                    j = r2.json()
                    return j.get("result", {}).get("message_id")
                else:
                    logger.error("Telegram 2ª tentativa falhou: %s", r2.text)
            else:
                logger.error("Telegram erro %s: %s", r.status_code, desc)
        except requests.HTTPError as he:
            logger.error("HTTPError Telegram: %s", getattr(he.response, "text", str(he)))
        except Exception as e:
            logger.error("Erro Telegram: %s", e)
        return None
