
from __future__ import annotations
from typing import Any, Dict, Iterable, Optional

def compute_final_score(ia_score: float, discount_rate: Optional[float], shop_trust: bool) -> float:
    d = max(0.0, min(1.0, (discount_rate or 0.0)))
    trust = 1.0 if shop_trust else 0.0
    return 0.6*ia_score + 0.25*(d*100.0) + 0.15*(trust*100.0)

def is_trusted_shop(shop_id: Optional[int], trusted_ids: Iterable[int]) -> bool:
    try: return int(shop_id) in set(int(x) for x in trusted_ids)
    except Exception: return False

def normalize_product_for_score(prod: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "itemId": prod.get("itemId") or prod.get("item_id"),
        "discountRate": prod.get("priceDiscountRate") or prod.get("discount") or 0.0,
        "shopId": prod.get("shopId") or prod.get("shop_id"),
    }
