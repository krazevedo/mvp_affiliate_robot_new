import os
import re
from typing import Optional

def _fmt_price(v) -> str:
    try:
        f = float(v)
        return f"R$ {f:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "-"


def _append_sub_id(url: str, sub_id: Optional[str]) -> str:
    if not url or not sub_id:
        return url or ""
    sep = "&" if "?" in url else "?"
    # Shopee usa subId ou sub_id dependendo do link. Preferimos subId.
    if "subId=" in url or "sub_id=" in url:
        return url
    return f"{url}{sep}subId={sub_id}"


def _strip_leading_name(body: str, product_name: str) -> str:
    """Remove o nome no início do corpo se ele já for o título."""
    if not body:
        return ""
    b = body.strip()
    pn = (product_name or "").strip()
    if not pn:
        return b
    # Se começa com o nome + ":" ou " - " removemos essa parte
    pattern = re.compile(rf"^\s*{re.escape(pn)}\s*[:\-–—]\s*", re.IGNORECASE)
    b = pattern.sub("", b, count=1)
    return b.strip()


class TelegramPublisher:
    def __init__(self, bot_token: str, chat_id: str):
        # Lazy import pra evitar dependência aqui
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._session = None

    def _get_session(self):
        if self._session is None:
            import requests
            from requests.adapters import HTTPAdapter, Retry
            s = requests.Session()
            retries = Retry(
                total=5, backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST"],
                respect_retry_after_header=True,
            )
            s.mount("https://", HTTPAdapter(max_retries=retries))
            self._session = s
        return self._session

    def build_message(
        self,
        *,
        product_name: str,
        texto_ia: str,
        price: float,
        shop: str,
        offer: str,
        rating: float | None = None,
        discount_rate: float | None = None,
        sales: int | None = None,
        badge: str | None = None,
        campaign: str | None = None,
        sub_id: str | None = None,
        cta_text: str = "🔗 Ver oferta",
        below_median_30d: bool = False,
        high_trust: bool = False,
    ) -> str:
        """Formata a mensagem final no padrão:
        **Título (nome)**
        Corpo: 1–2 linhas da IA/fallback (sem preço/estrelas/CTA)
        (linha opcional de porquê agora)
        Rodapé com preço/loja/estrelas/vendas
        CTA
        """
        title = f"**{product_name.strip()}**"

        body = _strip_leading_name((texto_ia or "").strip(), product_name)
        # Evita corpo vazio se strip removeu tudo
        if not body:
            body = "Benefício real para o dia a dia com ótimo custo‑benefício."

        why_now = None
        if below_median_30d:
            why_now = "Abaixo do preço mediano de 30 dias."
        elif isinstance(discount_rate, (int, float)) and float(discount_rate) >= 0.40:
            why_now = "Oferta forte hoje."
        elif isinstance(sales, int) and sales >= 1000:
            why_now = "Popular entre os compradores."

        preco = _fmt_price(price)
        stars = f"⭐ {rating:.1f}+" if isinstance(rating, (int, float)) and rating > 0 else None
        vendas = f"{sales}+ vendidos" if isinstance(sales, int) and sales > 0 else None
        trust = "Loja bem avaliada" if high_trust else None

        pieces = [title, body]
        if why_now:
            pieces.append(why_now)

        footer_lines = [f"Preço: {preco}", f"Loja: {shop}"]
        line_rating_sales = " • ".join([p for p in [stars, vendas] if p])
        if line_rating_sales:
            footer_lines.append(line_rating_sales)
        if trust:
            footer_lines.append(trust)

        pieces.append("\n".join(footer_lines))

        # CTA
        link = _append_sub_id(offer, sub_id)
        pieces.append(f"{cta_text}\n{link}")

        return "\n\n".join(pieces).strip()

    def send_message(self, text: str) -> str | None:
        import requests
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": False,
        }
        r = self._get_session().post(url, json=payload, timeout=(8, 20))
        r.raise_for_status()
        data = r.json()
        if data.get("ok") and data.get("result"):
            return str(data["result"]["message_id"])
        return None
