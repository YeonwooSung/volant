"""Client-side SCRAM proof computation (v0.46 SHA-256; v0.238 SHA-512).

Matches `crates/volant-client/src/scram.rs` and the broker AuthMessage
(no channel binding).
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
    return _proof_for(
        "sha256",
        hashlib.sha256,
        32,
        username,
        password,
        client_nonce,
        combined_nonce,
        salt,
        iterations,
    )


def client_proof_and_server_sig_sha512(
    username: str,
    password: str,
    client_nonce: str,
    combined_nonce: str,
    salt: bytes,
    iterations: int,
) -> tuple[bytes, bytes]:
    """Return ``(client_proof, expected_server_signature)`` (64 bytes each)."""
    return _proof_for(
        "sha512",
        hashlib.sha512,
        64,
        username,
        password,
        client_nonce,
        combined_nonce,
        salt,
        iterations,
    )


def _proof_for(
    pbkdf_name: str,
    digest,
    dklen: int,
    username: str,
    password: str,
    client_nonce: str,
    combined_nonce: str,
    salt: bytes,
    iterations: int,
) -> tuple[bytes, bytes]:
    salted = hashlib.pbkdf2_hmac(
        pbkdf_name, password.encode("utf-8"), salt, iterations, dklen=dklen
    )
    client_key = hmac.new(salted, b"Client Key", digest).digest()
    stored_key = digest(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", digest).digest()
    auth_message = _build_auth_message(
        username, client_nonce, combined_nonce, salt, iterations
    )
    client_signature = hmac.new(
        stored_key, auth_message.encode("utf-8"), digest
    ).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, client_signature))
    server_sig = hmac.new(
        server_key, auth_message.encode("utf-8"), digest
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
