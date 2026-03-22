from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
import streamlit as st

API_BASE_URL = "https://api.sumup.com/v0.1"


def _mask(value: str, keep: int = 4) -> str:
    text = str(value or "")
    if len(text) <= keep * 2:
        return "*" * len(text)
    return f"{text[:keep]}***{text[-keep:]}"


def _safe_json(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return value


@dataclass
class SumUpConfig:
    access_token: str
    merchant_code: str
    api_base_url: str = API_BASE_URL


class SumUpClient:
    def __init__(self, cfg: SumUpConfig, timeout_s: float = 20.0):
        self.cfg = cfg
        self.timeout_s = timeout_s

    @staticmethod
    def from_secrets() -> "SumUpClient":
        block = st.secrets.get("sumup", {})
        token = str(block.get("CLIENT_API_SECRET", "")).strip()
        merchant = str(block.get("MERCHANT_ID", "")).strip()
        return SumUpClient(SumUpConfig(access_token=token, merchant_code=merchant))

    def is_configured(self) -> bool:
        return bool(self.cfg.access_token and self.cfg.merchant_code)

    def config_debug(self) -> Dict[str, Any]:
        return {
            "api_base_url": self.cfg.api_base_url,
            "merchant_code": self.cfg.merchant_code,
            "access_token_masked": _mask(self.cfg.access_token),
            "configured": self.is_configured(),
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_payload: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        if not self.cfg.access_token:
            return {
                "ok": False,
                "status_code": 0,
                "error": "Missing SumUp access token.",
                "json": None,
                "text": "",
                "trace": {},
            }
        headers = {
            "Authorization": f"Bearer {self.cfg.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if extra_headers:
            headers.update({k: str(v) for k, v in extra_headers.items()})
        url = f"{self.cfg.api_base_url.rstrip('/')}/{path.lstrip('/')}"
        trace_headers = {k: v for k, v in headers.items() if k.lower() != "authorization"}
        trace_headers["Authorization"] = f"Bearer {_mask(self.cfg.access_token)}"
        start = time.perf_counter()
        try:
            response = requests.request(
                method=method.upper(),
                url=url,
                params=params or None,
                json=json_payload if json_payload is not None else None,
                headers=headers,
                timeout=self.timeout_s,
            )
            elapsed_ms = round((time.perf_counter() - start) * 1000.0, 1)
            parsed_json: Any = None
            try:
                parsed_json = response.json()
            except Exception:
                parsed_json = None
            return {
                "ok": response.status_code < 400,
                "status_code": response.status_code,
                "json": parsed_json,
                "text": response.text or "",
                "error": "" if response.status_code < 400 else (response.text or "API error"),
                "trace": {
                    "method": method.upper(),
                    "url": url,
                    "params": params or {},
                    "json_payload": json_payload or {},
                    "headers": trace_headers,
                    "elapsed_ms": elapsed_ms,
                    "status_code": response.status_code,
                },
            }
        except requests.RequestException as exc:
            elapsed_ms = round((time.perf_counter() - start) * 1000.0, 1)
            return {
                "ok": False,
                "status_code": 0,
                "json": None,
                "text": "",
                "error": str(exc),
                "trace": {
                    "method": method.upper(),
                    "url": url,
                    "params": params or {},
                    "json_payload": json_payload or {},
                    "headers": trace_headers,
                    "elapsed_ms": elapsed_ms,
                    "status_code": 0,
                },
            }

    def me(self, *, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        return self._request("GET", "/me", extra_headers=extra_headers)

    def transaction_history(
        self,
        *,
        limit: int = 20,
        statuses: Optional[List[str]] = None,
        tx_types: Optional[List[str]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": int(limit), "order": "descending"}
        if statuses:
            params["statuses[]"] = statuses
        if tx_types:
            params["types[]"] = tx_types
        return self._request(
            "GET",
            "/me/transactions/history",
            params=params,
            extra_headers=extra_headers,
        )

    def transaction_details(
        self, tx_id: str, *, extra_headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        return self._request(
            "GET",
            "/me/transactions",
            params={"id": tx_id},
            extra_headers=extra_headers,
        )

    def create_checkout(
        self,
        *,
        amount: float,
        currency: str,
        checkout_reference: str,
        description: str,
        metadata: Optional[Dict[str, Any]] = None,
        return_url: Optional[str] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "checkout_reference": checkout_reference,
            "amount": float(amount),
            "currency": currency,
            "merchant_code": self.cfg.merchant_code,
            "description": description,
        }
        if return_url:
            payload["return_url"] = return_url
        if metadata:
            payload["metadata"] = metadata
        return self._request(
            "POST",
            "/checkouts",
            json_payload=payload,
            extra_headers=extra_headers,
        )

    def checkout_details(
        self, checkout_id: str, *, extra_headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/checkouts/{checkout_id}",
            extra_headers=extra_headers,
        )


def build_tx_stats(history_payload: Any) -> Dict[str, Any]:
    items = []
    if isinstance(history_payload, dict):
        if isinstance(history_payload.get("items"), list):
            items = history_payload.get("items", [])
        elif isinstance(history_payload.get("transactions"), list):
            items = history_payload.get("transactions", [])
    elif isinstance(history_payload, list):
        items = history_payload

    totals_by_currency: Dict[str, float] = {}
    count_by_status: Dict[str, int] = {}
    latest_ts = ""
    earliest_ts = ""
    for item in items:
        if not isinstance(item, dict):
            continue
        currency = str(item.get("currency") or "UNK")
        amount_raw = item.get("amount")
        try:
            amount = float(amount_raw)
        except Exception:
            amount = 0.0
        totals_by_currency[currency] = round(totals_by_currency.get(currency, 0.0) + amount, 2)
        status = str(item.get("status") or "UNKNOWN")
        count_by_status[status] = count_by_status.get(status, 0) + 1
        ts = str(item.get("timestamp") or "")
        if ts:
            latest_ts = max(latest_ts, ts) if latest_ts else ts
            earliest_ts = min(earliest_ts, ts) if earliest_ts else ts
    return {
        "count": len(items),
        "totals_by_currency": totals_by_currency,
        "count_by_status": count_by_status,
        "latest_timestamp": latest_ts,
        "earliest_timestamp": earliest_ts,
    }


def parse_metadata_text(metadata_text: str) -> Dict[str, Any]:
    raw = str(metadata_text or "").strip()
    if not raw:
        return {}
    parsed = _safe_json(raw)
    if isinstance(parsed, dict):
        return parsed
    raise ValueError("Metadata must be a JSON object.")
