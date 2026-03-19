from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any


SESSION_TTL_SECONDS = 24 * 60 * 60
PUBLIC_VIEW_TTL_SECONDS = 7 * 24 * 60 * 60


def dashboard_password() -> str:
    return os.getenv("DASHBOARD_PASSWORD", "admin123")


def _secret_key() -> bytes:
    return hashlib.sha256(f"seo-dashboard::{dashboard_password()}".encode("utf-8")).digest()


def _encode_signed_payload(payload: dict[str, Any]) -> str:
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    signature = hmac.new(_secret_key(), payload_bytes, hashlib.sha256).hexdigest()
    token = {
        "payload": base64.urlsafe_b64encode(payload_bytes).decode("utf-8").rstrip("="),
        "sig": signature,
    }
    return base64.urlsafe_b64encode(json.dumps(token).encode("utf-8")).decode("utf-8").rstrip("=")


def _decode_signed_payload(token: str) -> dict[str, Any]:
    padding = "=" * (-len(token) % 4)
    decoded = base64.urlsafe_b64decode(f"{token}{padding}".encode("utf-8"))
    data = json.loads(decoded.decode("utf-8"))
    payload_b64 = data["payload"]
    payload_padding = "=" * (-len(payload_b64) % 4)
    payload_bytes = base64.urlsafe_b64decode(f"{payload_b64}{payload_padding}".encode("utf-8"))
    expected = hmac.new(_secret_key(), payload_bytes, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, data["sig"]):
        raise ValueError("Invalid signature")
    return json.loads(payload_bytes.decode("utf-8"))


def create_token() -> str:
    expires_at = int(time.time()) + SESSION_TTL_SECONDS
    payload = {"exp": expires_at, "scope": "dashboard"}
    return _encode_signed_payload(payload)


def verify_token(token: str) -> dict[str, Any]:
    payload = _decode_signed_payload(token)
    if int(payload.get("exp", 0)) < int(time.time()):
        raise ValueError("Session expired")
    if payload.get("scope") != "dashboard":
        raise ValueError("Invalid scope")
    return payload


def hash_view_password(password: str) -> str:
    return hashlib.sha256(f"public-view::{password}".encode("utf-8")).hexdigest()


def verify_view_password(password: str, expected_hash: str | None) -> bool:
    if not expected_hash:
        return True
    return hmac.compare_digest(hash_view_password(password), expected_hash)


def create_public_view_token(share_token: str) -> str:
    expires_at = int(time.time()) + PUBLIC_VIEW_TTL_SECONDS
    payload = {
        "exp": expires_at,
        "scope": "public_view",
        "share_token": share_token,
    }
    return _encode_signed_payload(payload)


def verify_public_view_token(token: str, share_token: str) -> dict[str, Any]:
    payload = _decode_signed_payload(token)
    if int(payload.get("exp", 0)) < int(time.time()):
        raise ValueError("Session expired")
    if payload.get("scope") != "public_view":
        raise ValueError("Invalid scope")
    if payload.get("share_token") != share_token:
        raise ValueError("Share token mismatch")
    return payload
