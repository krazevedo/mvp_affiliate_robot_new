
"""
ai.py — IA com saída estruturada (A/B) e penalidades.
Gera para cada item: { itemId, pontuacao, texto_de_venda_a, texto_de_venda_b }
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ValidationError, Field
try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated

# ----- Modelos de saída -----
class IAItem(BaseModel):
    itemId: int
    pontuacao: Annotated[int, Field(ge=0, le=100)]
    texto_de_venda_a: str
    texto_de_venda_b: str

class IAResponse(BaseModel):
    analise_de_produtos: List[IAItem]

JSON_BLOCK_RE = re.compile(r"\{(?:[^{}]|(?R))*\}", re.DOTALL)

def largest_json_block(text: str) -> Optional[str]:
    blocks = JSON_BLOCK_RE.findall(text or "")
    if not blocks:
        return None
    return max(blocks, key=len)

def try_parse_ia(text: str) -> Optional[IAResponse]:
    if not text:
        return None
    candidate = largest_json_block(text)
    if not candidate:
        return None
    def _attempt(s: str):
        try:
            data = json.loads(s)
        except Exception:
            return None
        try:
            # Tolerar modelos que devolvem só "texto_de_venda": duplicamos em A/B
            if isinstance(data, dict) and "analise_de_produtos" in data:
                for it in data["analise_de_produtos"]:
                    if "texto_de_venda" in it and ("texto_de_venda_a" not in it or "texto_de_venda_b" not in it):
                        it["texto_de_venda_a"] = it.get("texto_de_venda")
                        it["texto_de_venda_b"] = it.get("texto_de_venda")
            return IAResponse.model_validate(data)
        except ValidationError:
            return None
    parsed = _attempt(candidate)
    if parsed:
        return parsed
    # autocorreção simples: remover trailing vírgulas
    sanitized = re.sub(r",\s*([}\]])", r"\1", candidate)
    return _attempt(sanitized)

def call_gemini(prompt: str, *, model: str = "gemini-1.5-flash", api_key: Optional[str] = None) -> str:
    if api_key is None:
        import os
        api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY não configurada.")
    try:
        import google.generativeai as genai
    except ImportError as e:
        raise RuntimeError("Pacote google-generativeai não instalado. `pip install google-generativeai`") from e
    genai.configure(api_key=api_key)
    gmodel = genai.GenerativeModel(model)
    resp = gmodel.generate_content(prompt)
    return getattr(resp, "text", None) or ""

def analyze_products(products: List[Dict[str, Any]], *, model: str = "gemini-1.5-flash", api_key: Optional[str] = None) -> IAResponse:
    compact = [
        {
            "itemId": p.get("itemId") or p.get("item_id"),
            "name": p.get("productName") or p.get("name") or p.get("itemName"),
            "ratingStar": p.get("ratingStar") or p.get("rating"),
            "sales": p.get("sales"),
            "priceMin": p.get("priceMin"),
            "priceMax": p.get("priceMax"),
            "discountRate": p.get("priceDiscountRate") or p.get("discount"),
            "link": p.get("productLink") or p.get("link"),
        }
        for p in products
    ]

    # Regras claras no prompt (penalidades)
    system = (
        "Você é um curador de ofertas. Retorne SOMENTE JSON válido no schema:\n"
        "{ \"analise_de_produtos\": [ { \"itemId\": int, \"pontuacao\": 0-100, \"texto_de_venda_a\": str, \"texto_de_venda_b\": str } ] }\n\n"
        "Critérios:\n"
        "- Dê notas MAIS ALTAS para: (a) alta reputação (rating>=4.6), (b) desconto real alto, (c) utilidade clara comprovada, (d) preço competitivo na categoria.\n"
        "- Dê notas BAIXAS (<=40) para produtos com promessas suspeitas ou termos problemáticos: PMPO, 4K barato/\"4K suporte\" em projetor genérico, i12/iXX TWS clone,\n"
        "  'câmera espiã'/'espi', 'ChatGPT' em smartwatch barato, claims exagerados de potência. Se possível, reflita isso na nota.\n\n"
        "Texto de venda (A e B):\n"
        "- Duas variações curtas (até 160 caracteres), sem emojis, sem links e sem promessas não verificáveis.\n"
        "- A: foco em benefício/uso; B: foco em prova social (rating/vendas) ou urgência leve.\n"
    )
    user = f"Produtos:\n{json.dumps(compact, ensure_ascii=False)}\nRetorne SOMENTE JSON (sem texto fora do JSON)."
    prompt = f"{system}\n\n{user}"
    raw = call_gemini(prompt, model=model, api_key=api_key)
    parsed = try_parse_ia(raw)

    if not parsed:
        repair_prompt = (
            f"{system}\n\nO JSON anterior estava inválido. Gere novamente, estritamente válido, sem comentários.\n"
            f"{user}"
        )
        raw2 = call_gemini(repair_prompt, model=model, api_key=api_key)
        parsed = try_parse_ia(raw2)

    if not parsed:
        fallback = []
        for p in compact:
            iid = p.get("itemId")
            if not iid: 
                continue
            name = (p.get("name") or "Oferta").strip()
            fallback.append({
                "itemId": int(iid),
                "pontuacao": 50,
                "texto_de_venda_a": f"{name}: bom custo-benefício pra usar no dia a dia. Aproveite!",
                "texto_de_venda_b": f"{name}: destaque entre os mais buscados. Pegue com desconto enquanto dura.",
            })
        return IAResponse(analise_de_produtos=[IAItem(**x) for x in fallback])

    return parsed
