# -*- coding: utf-8 -*-
from __future__ import annotations

import html
import re
from typing import Optional

EMOJI_BY_CAT = {
    "mouse/teclado/periféricos": "🖱️",
    "smartwatch/wearables": "⌚",
    "caixa de som/speaker": "🔊",
    "projetor": "📽️",
    "cozinha (airfryer etc.)": "🍳",
    "câmera/segurança": "📷",
    "papelaria": "📝",
    "outros": "✨",
}

CTA_LABELS = {
    "A": "🔗 Ver oferta",
    "B": "🔗 Abrir no app",
}

TITLE_PREFIXES_TO_STRIP = [
    r"super oferta\s*-\s*",
    r"oferta relâmpago\s*-\s*",
]

TITLE_NOISE = [
    "original", "usb", "com fio", "sem fio", "rgb", "led",
]

def _clean_title(name: str) -> str:
    t = (name or "").strip()
    for pat in TITLE_PREFIXES_TO_STRIP:
        t = re.sub(pat, "", t, flags=re.IGNORECASE)
    # compactar espaços e capitalização leve (sem gritar)
    t = re.sub(r"\s{2,}", " ", t)
    return t.strip()

def _sanitize_body(text: str, product_name: str) -> str:
    # evita repetir o nome do produto no corpo se já estiver no título
    t = (text or "").strip()
    # remove repetições do nome no início
    pname = (product_name or "").strip()
    if pname and t.lower().startswith(pname.lower()):
        t = t[len(pname):].lstrip(": ").lstrip("- ").strip()
    # tira excesso de espaços e pontuação
    t = re.sub(r"\s{2,}", " ", t)
    t = re.sub(r"[.!?]{2,}$", ".", t)
    # reduz frases genéricas demais
    t = t.replace("para o dia a dia com ótimo custo-benefício", "com ótimo custo-benefício no dia a dia")
    return t.strip()

def _format_price_brl(value: float) -> str:
    if value is None:
        return "-"
    # formatação PT-BR simples
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _reason_now(discount_rate: Optional[float], below_median_30d: bool, sales: Optional[int]) -> Optional[str]:
    try:
        disc = float(discount_rate or 0.0)
    except Exception:
        disc = 0.0
    s = int(sales) if (isinstance(sales, (int, float, str)) and str(sales).isdigit()) else 0

    if below_median_30d:
        return "Preço DESPENCOU"
    if disc >= 0.40:
        return "Só hoje."
    if s >= 1000:
        return "Está todo mundo comprando."
    return None

def _append_subid(url: str, sub_id: str) -> str:
    if not url:
        return url
    sep = "&" if "?" in url else "?"
    if "sub_id=" in url:
        return url
    return f"{url}{sep}sub_id={sub_id}"

class TelegramPublisher:
    def __init__(self, *, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id

    def build_message(
        self,
        *,
        product_name: str,
        texto_ia: str,
        price: float,
        shop: str,
        offer: str,
        rating: Optional[float],
        discount_rate: Optional[float],
        sales: Optional[int],
        badge: Optional[str],
        campaign: str,
        sub_id: str,
        category: str = "outros",
        below_median_30d: bool = False,
        cta_variant: str = "A",
    ) -> str:
        # título
        title = _clean_title(product_name)
        emoji = EMOJI_BY_CAT.get(category, "✨")
        title_line = f"**{title}** {emoji}"

        # corpo
        body = _sanitize_body(texto_ia, product_name)
        reason = _reason_now(discount_rate, below_median_30d, sales)
        if reason:
            body = f"{body}\n\n{reason}" if body else reason

        # rodapé informacional
        price_txt = _format_price_brl(price if price is not None else 0.0)
        shop_txt = (shop or "").strip()
        stars_sales_parts = []
        if rating not in (None, ""):
            try:
                stars_sales_parts.append(f"⭐ {float(rating):.1f}+")
            except Exception:
                pass
        if isinstance(sales, (int, float)) or (isinstance(sales, str) and sales.isdigit()):
            stars_sales_parts.append(f"{int(sales)}+ vendidos")
        stars_sales = " • ".join(stars_sales_parts) if stars_sales_parts else ""
        trust = ""
        try:
            if rating is not None and float(rating) >= 4.8 and (int(sales or 0) >= 100):
                trust = "\n(Loja bem avaliada)"
        except Exception:
            trust = ""

        # CTA
        cta_text = CTA_LABELS.get(cta_variant.upper(), CTA_LABELS["A"])
        url = _append_subid(offer, sub_id)
        cta_line = f"{cta_text}\n{url}"

        parts = [
            title_line.strip(),
            body.strip(),
            f"\nPreço: {price_txt}\nLoja: {shop_txt}",
            f"{stars_sales}".strip(),
            trust.strip(),
            cta_line.strip(),
        ]
        # remova linhas vazias dobradas
        msg = "\n".join([p for p in parts if p])
        msg = re.sub(r"\n{3,}", "\n\n", msg).strip()
        return msg

    # Placeholder de envio real — você já tinha um send_message no seu projeto
    def send_message(self, message: str) -> Optional[str]:
        import requests, os
        token = self.bot_token
        chat_id = self.chat_id
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": False,
        }
        r = requests.post(url, json=payload, timeout=20)
        r.raise_for_status()
        data = r.json()
        if data.get("ok") and data.get("result", {}).get("message_id"):
            return str(data["result"]["message_id"])
        return None
