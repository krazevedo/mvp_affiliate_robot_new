# shopee_monorepo_modules/conversions.py
from __future__ import annotations

import time, json, hashlib
from typing import Dict, Iterator, Optional, List
import requests

AFFILIATE_ENDPOINT = "https://open-api.affiliate.shopee.com.br/graphql"
USER_AGENT = "OfferBot/1.3 (+https://github.com/yourrepo)"

def _auth_header(partner_id: int, api_key: str, payload: str) -> str:
    ts = int(time.time())
    sig = hashlib.sha256(f"{partner_id}{ts}{payload}{api_key}".encode("utf-8")).hexdigest()
    return f"SHA256 Credential={partner_id}, Timestamp={ts}, Signature={sig}"

def make_session() -> requests.Session:
    from requests.adapters import HTTPAdapter, Retry
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET", "POST"],
                    respect_retry_after_header=True)
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": USER_AGENT})
    return s

def _build_args(
    *,
    purchase_start: Optional[int] = None,
    purchase_end: Optional[int] = None,
    complete_start: Optional[int] = None,
    complete_end: Optional[int] = None,
    shop_name: Optional[str] = None,
    shop_id: Optional[int] = None,
    device: Optional[str] = None,
    buyer_type: Optional[str] = None,
    order_status: Optional[str] = None,
    campaign_type: Optional[str] = None,
    limit: int = 500,
    scroll_id: Optional[str] = None,
) -> str:
    parts: List[str] = []
    if purchase_start is not None: parts.append(f"purchaseTimeStart: {int(purchase_start)}")
    if purchase_end is not None: parts.append(f"purchaseTimeEnd: {int(purchase_end)}")
    if complete_start is not None: parts.append(f"completeTimeStart: {int(complete_start)}")
    if complete_end is not None: parts.append(f"completeTimeEnd: {int(complete_end)}")
    if shop_name: parts.append(f'shopName: "{shop_name}"')
    if shop_id is not None: parts.append(f"shopId: {int(shop_id)}")
    if device: parts.append(f'device: "{device}"')
    if buyer_type: parts.append(f'buyerType: "{buyer_type}"')
    if order_status: parts.append(f'orderStatus: "{order_status}"')
    if campaign_type: parts.append(f'campaignType: "{campaign_type}"')
    parts.append(f"limit: {int(limit)}")
    if scroll_id: parts.append(f'scrollId: "{scroll_id}"')
    return ", ".join(parts)

CONVERSION_FIELDS = """
purchaseTime clickTime conversionId buyerType device utmContent referrer
  netCommission totalCommission
  orders {
    orderId orderStatus shopType
    items {
      shopId shopName completeTime itemId itemName itemPrice displayItemStatus actualAmount qty imageUrl
      itemTotalCommission itemSellerCommission itemShopeeCommissionCapped
      itemSellerCommissionRate itemShopeeCommissionRate
      fraudStatus channelType attributionType
      globalCategoryLv1Name globalCategoryLv2Name globalCategoryLv3Name
      modelId promotionId
    }
  }
""".strip()

def iter_conversion_report(
    session: requests.Session,
    partner_id: int,
    api_key: str,
    *,
    purchase_start: Optional[int] = None,
    purchase_end: Optional[int] = None,
    complete_start: Optional[int] = None,
    complete_end: Optional[int] = None,
    limit: int = 500,
) -> Iterator[Dict]:
    """
    Itera nós de conversionReport lidando com scrollId (30s de validade).
    Use preferencialmente um par de janelas: purchase_* OU complete_*.
    """
    scroll_id: Optional[str] = None
    while True:
        args = _build_args(
            purchase_start=purchase_start, purchase_end=purchase_end,
            complete_start=complete_start, complete_end=complete_end,
            limit=limit, scroll_id=scroll_id
        )
        query = f"query {{ conversionReport({args}) {{ nodes {{ {CONVERSION_FIELDS} }} pageInfo {{ hasNextPage scrollId limit }} }} }}"
        body = {"query": query, "variables": {}}
        payload = json.dumps(body, separators=(",", ":"))
        headers = {"Authorization": _auth_header(partner_id, api_key, payload),
                   "Content-Type": "application/json"}
        r = session.post(AFFILIATE_ENDPOINT, data=payload, headers=headers, timeout=(8, 30))
        r.raise_for_status()
        data = r.json()
        if "errors" in data and data["errors"]:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        root = data["data"]["conversionReport"]
        for node in root["nodes"]:
            yield node
        page = root["pageInfo"]
        if not page.get("hasNextPage"):
            break
        sid = page.get("scrollId")
        if not sid:
            break
        scroll_id = sid
