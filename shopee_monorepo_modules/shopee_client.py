# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
import hashlib
import logging
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry

LOGGER = logging.getLogger("shopee_client")

GRAPHQL_URL = "https://open-api.affiliate.shopee.com.br/graphql"
GRAPHQL_PATH = "/graphql"
UA = "Mozilla/5.0 (compatible; ShopeeAffiliateBot/2.0; +github-actions)"

def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "HEAD"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": UA})
    return s

class ShopeeClient:
    """
    Cliente resiliente para a GraphQL de Afiliados da Shopee.

    Assinaturas testadas (em ordem):
      - v2_payload: sha256(partner_id + ts + payload_json + api_key)
      - v3_path:    sha256(partner_id + ts + "/graphql" + payload_json + api_key)
      - v1_min:     sha256(partner_id + ts + api_key)
    """
    def __init__(self, partner_id: str, api_key: str, session: Optional[requests.Session] = None) -> None:
        self.partner_id = partner_id.strip()
        self.api_key = api_key.strip()
        self.session = session or _make_session()
        self.last_auth_mode: Optional[str] = None  # lembra o modo que funcionou

    # ---- Assinaturas --------------------------------------------------------
    def _auth_header(self, payload: Dict[str, Any], mode: str) -> str:
        ts = int(time.time())
        payload_str = json.dumps(payload, separators=(",", ":"))
        if mode == "v2_payload":
            base = f"{self.partner_id}{ts}{payload_str}{self.api_key}"
        elif mode == "v3_path":
            base = f"{self.partner_id}{ts}{GRAPHQL_PATH}{payload_str}{self.api_key}"
        elif mode == "v1_min":
            base = f"{self.partner_id}{ts}{self.api_key}"
        else:
            raise ValueError(f"Modo de assinatura inválido: {mode}")
        sign = hashlib.sha256(base.encode("utf-8")).hexdigest()
        return f"SHA256 Credential={self.partner_id}, Timestamp={ts}, Signature={sign}"

    def _post_graphql_auto(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        body = {"query": query, "variables": variables or {}}
        # prioriza o último modo válido:
        modes = ["v2_payload", "v3_path", "v1_min"]
        if self.last_auth_mode in modes:
            modes.remove(self.last_auth_mode)
            modes.insert(0, self.last_auth_mode)

        for mode in modes:
            headers = {
                "Authorization": self._auth_header(body, mode),
                "Content-Type": "application/json",
            }
            try:
                resp = self.session.post(GRAPHQL_URL, json=body, headers=headers, timeout=20)
                resp.raise_for_status()
                data = resp.json()
            except requests.HTTPError as e:
                # Se for 401/403, tente próximo modo
                if e.response is not None and e.response.status_code in (401, 403):
                    LOGGER.warning("HTTP %s com modo %s — tentando próximo", e.response.status_code, mode)
                    continue
                raise

            # Erros GraphQL
            if "errors" in data and data["errors"]:
                # Verifica assinatura inválida
                msg = json.dumps(data["errors"], ensure_ascii=False)
                if "Invalid Signature" in msg or "Invalid Authorization Header" in msg:
                    LOGGER.warning("GraphQL (%s) retornou assinatura inválida — tentando próximo modo", mode)
                    continue
                # Outros erros: retorna para o chamador lidar
                return data

            # Sucesso
            self.last_auth_mode = mode
            return data

        # Se todos os modos falharem com assinatura inválida
        raise RuntimeError("Falha de autenticação: todos os modos de assinatura retornaram 'Invalid Signature'.")

    # ---- Consultas de produtos ---------------------------------------------
    def product_offer_v2_by_keyword(self, keyword: str, *, page: int = 1, limit: int = 15) -> List[Dict[str, Any]]:
        kw = keyword.replace('"', '\\"')
        query = (
            "query { productOfferV2("
            f'keyword: "{kw}", limit: {int(limit)}, page: {int(page)}'
            ") { nodes { "
            "itemId productName priceMin priceMax offerLink productLink "
            "shopName ratingStar sales priceDiscountRate } } }"
        )
        data = self._post_graphql_auto(query)
        return (data.get("data", {})
                    .get("productOfferV2", {})
                    .get("nodes", [])) or []

    def product_offer_v2_by_shop(self, shop_id: int, *, page: int = 1, limit: int = 15) -> List[Dict[str, Any]]:
        query = (
            "query { productOfferV2("
            f"shopId: {int(shop_id)}, limit: {int(limit)}, page: {int(page)}"
            ") { nodes { "
            "itemId productName priceMin priceMax offerLink productLink "
            "shopName ratingStar sales priceDiscountRate } } }"
        )
        data = self._post_graphql_auto(query)
        return (data.get("data", {})
                    .get("productOfferV2", {})
                    .get("nodes", [])) or []