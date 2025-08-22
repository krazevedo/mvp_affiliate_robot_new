# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
import hmac
import hashlib
import logging
import os
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

def _hmac_sha256_hex(secret: str, message: str) -> str:
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()

class ShopeeClient:
    """
    Cliente resiliente para a GraphQL de Afiliados da Shopee.

    Assinaturas testadas (todas com HMAC-SHA256 usando api_key como *secret*):
      - v2_payload: sign(partner_id + ts + payload_json)
      - v3_path:    sign(partner_id + ts + "/graphql" + payload_json)
      - v1_min:     sign(partner_id + ts)

    Você pode forçar um modo setando SHOPEE_AUTH_MODE = v2_payload | v3_path | v1_min
    """
    def __init__(self, partner_id: str, api_key: str, session: Optional[requests.Session] = None) -> None:
        self.partner_id = partner_id.strip()
        self.api_key = api_key.strip()
        self.session = session or _make_session()
        self.last_auth_mode: Optional[str] = None

        forced = os.getenv("SHOPEE_AUTH_MODE", "").strip()
        self.forced_mode = forced if forced in ("v2_payload", "v3_path", "v1_min") else None
        if self.forced_mode:
            LOGGER.info("Forçando modo de assinatura: %s", self.forced_mode)

    # ---- Assinaturas (HMAC) -------------------------------------------------
    def _auth_header(self, payload: Dict[str, Any], mode: str, ts: int) -> str:
        payload_str = json.dumps(payload, separators=(",", ":"))
        if mode == "v2_payload":
            base = f"{self.partner_id}{ts}{payload_str}"
        elif mode == "v3_path":
            base = f"{self.partner_id}{ts}{GRAPHQL_PATH}{payload_str}"
        elif mode == "v1_min":
            base = f"{self.partner_id}{ts}"
        else:
            raise ValueError(f"Modo de assinatura inválido: {mode}")
        sign = _hmac_sha256_hex(self.api_key, base)
        return f"SHA256 Credential={self.partner_id}, Timestamp={ts}, Signature={sign}"

    def _post_graphql_auto(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        body = {"query": query, "variables": variables or {}}
        modes = ["v2_payload", "v3_path", "v1_min"]

        # Força um modo? Coloca ele primeiro e ignora o resto na falha de 401/403/Invalid Signature
        if self.forced_mode:
            modes = [self.forced_mode]
        else:
            if self.last_auth_mode in modes:
                modes.remove(self.last_auth_mode)
                modes.insert(0, self.last_auth_mode)

        for mode in modes:
            ts = int(time.time())  # segundos
            headers = {
                "Authorization": self._auth_header(body, mode, ts),
                "Content-Type": "application/json",
            }
            try:
                resp = self.session.post(GRAPHQL_URL, json=body, headers=headers, timeout=20)
                resp.raise_for_status()
                data = resp.json()
            except requests.HTTPError as e:
                # 401/403 geralmente é assinatura -> tenta próximo modo
                code = e.response.status_code if e.response is not None else None
                if code in (401, 403):
                    LOGGER.warning("HTTP %s com modo %s — tentando próximo", code, mode)
                    continue
                raise

            # Erros GraphQL (estrutura padronizada)
            if isinstance(data, dict) and data.get("errors"):
                msg = json.dumps(data["errors"], ensure_ascii=False)
                if "Invalid Signature" in msg or "Invalid Authorization Header" in msg:
                    LOGGER.warning("GraphQL (%s) retornou assinatura inválida — tentando próximo modo", mode)
                    continue
                # outros erros: devolve para o chamador lidar
                return data

            # Sucesso
            self.last_auth_mode = mode
            return data

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
