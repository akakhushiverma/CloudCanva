"""
Cognito JWT verification middleware for FastAPI.
Validates the access token from the Authorization header against
the Cognito User Pool's public keys (JWKS).
"""
import os
import time
import requests
from typing import Optional
from jose import jwt, JWTError, jwk
from fastapi import Request, HTTPException
from functools import lru_cache

# Configuration — set these as env vars or update defaults
COGNITO_REGION = os.environ.get("COGNITO_REGION", "ap-south-1")
COGNITO_USER_POOL_ID = os.environ.get("COGNITO_USER_POOL_ID", "ap-south-1_p3FS3EO3U")
COGNITO_APP_CLIENT_ID = os.environ.get("COGNITO_APP_CLIENT_ID", "6euggkqooj1g9fb5r4lr4l949u")

JWKS_URL = f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}/.well-known/jwks.json"
ISSUER = f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}"

# Cache JWKS keys
_jwks_cache: dict = {"keys": [], "fetched_at": 0}
JWKS_CACHE_TTL = 3600  # 1 hour


def _get_jwks() -> list:
    """Fetch and cache JWKS from Cognito."""
    now = time.time()
    if _jwks_cache["keys"] and (now - _jwks_cache["fetched_at"]) < JWKS_CACHE_TTL:
        return _jwks_cache["keys"]

    try:
        response = requests.get(JWKS_URL, timeout=5)
        response.raise_for_status()
        keys = response.json().get("keys", [])
        _jwks_cache["keys"] = keys
        _jwks_cache["fetched_at"] = now
        return keys
    except Exception as e:
        print(f"Failed to fetch JWKS: {e}")
        # Return cached keys if available, even if stale
        return _jwks_cache["keys"]


def _get_public_key(token: str):
    """Get the public key that matches the token's kid header."""
    try:
        headers = jwt.get_unverified_headers(token)
    except JWTError:
        return None

    kid = headers.get("kid")
    if not kid:
        return None

    keys = _get_jwks()
    for key in keys:
        if key.get("kid") == kid:
            return key
    return None


def verify_token(token: str) -> dict:
    """
    Verify and decode a Cognito access token.
    Returns the decoded claims if valid.
    Raises HTTPException if invalid.
    """
    public_key = _get_public_key(token)
    if not public_key:
        raise HTTPException(status_code=401, detail="Invalid token: key not found")

    try:
        claims = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience=COGNITO_APP_CLIENT_ID,
            issuer=ISSUER,
            options={
                "verify_aud": False,  # access tokens don't have aud in Cognito
                "verify_iss": True,
                "verify_exp": True,
            },
        )
        # Verify token_use is "access"
        if claims.get("token_use") != "access":
            raise HTTPException(status_code=401, detail="Invalid token type")
        return claims
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")


def get_current_user(request: Request) -> dict:
    """
    Extract and verify the bearer token from the request.
    Returns decoded token claims.
    Use as a FastAPI dependency.
    """
    # Dev mode: skip auth when SKIP_AUTH=true (local development only)
    if os.environ.get("SKIP_AUTH", "").lower() == "true":
        return {"username": "dev-user", "sub": "dev-user-id"}

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization header")

    token = auth_header.split("Bearer ")[1]
    return verify_token(token)
