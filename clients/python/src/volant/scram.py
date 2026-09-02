"""Client-side SCRAM-SHA-256 proof computation (v0.46).

Matches `crates/volant-client/src/scram.rs` and the broker AuthMessage
(no channel binding, SHA-256 only).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os


def generate_client_nonce() -> str:
    """16 random bytes, standard Base64, ``,`` replaced with ``A``."""
    return base64.b64encode(os.urandom(16)).decode("ascii").replace(",", "A")


def client_proof_and_server_sig(
    username: str,
    password: str,
    client_nonce: str,
    combined_nonce: str,
    salt: bytes,
    iterations: int,
) -> tuple[bytes, bytes]:
    """Return ``(client_proof, expected_server_signature)`` (32 bytes each)."""
    salted = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, iterations, dklen=32
    )
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    auth_message = _build_auth_message(
        username, client_nonce, combined_nonce, salt, iterations
    )
    client_signature = hmac.new(
        stored_key, auth_message.encode("utf-8"), hashlib.sha256
    ).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, client_signature))
    server_sig = hmac.new(
        server_key, auth_message.encode("utf-8"), hashlib.sha256
    ).digest()
    return proof, server_sig


def _build_auth_message(
    username: str,
    client_nonce: str,
    combined_nonce: str,
    salt: bytes,
    iterations: int,
) -> str:
    client_first_bare = f"n={username},r={client_nonce}"
    server_first = "r={0},s={1},i={2}".format(
        combined_nonce,
        base64.b64encode(salt).decode("ascii"),
        iterations,
    )
    client_final_wo_proof = f"c=biws,r={combined_nonce}"
    return f"{client_first_bare},{server_first},{client_final_wo_proof}"
