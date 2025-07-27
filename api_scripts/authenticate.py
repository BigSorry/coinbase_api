import os
import jwt
import time
import json
import secrets
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

# Load environment variables from .env file
load_dotenv()
# Access the API key and private key from environment variables
API_KEY_ID = os.getenv("API_KEY_ID")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")

def build_jwt(jwt_payload):
    private_key_bytes = PRIVATE_KEY.encode('utf-8')
    private_key = serialization.load_pem_private_key(private_key_bytes, password=None)

    jwt_token = jwt.encode(
        jwt_payload,
        private_key,
        algorithm='ES256',
        headers={'kid': API_KEY_ID, 'nonce': secrets.token_hex()},
    )

    return jwt_token

def getJWT(request_method, request_host, request_path):
    uri = f"{request_method} {request_host}{request_path}"
    jwt_payload = {
        'sub': API_KEY_ID,
        'iss': "cdp",
        'nbf': int(time.time()),
        'exp': int(time.time()) + 120,
        'uri': uri,
    }
    jwt_token = build_jwt(jwt_payload)

    return jwt_token

def postJWT(request_method, request_host, request_path, body_data):
    minified_body = json.dumps(body_data, separators=(",", ":"))
    uri = f"{request_method} {request_host}{request_path}"
    jwt_payload = {
        'sub': API_KEY_ID,
        'iss': "cdp",
        'nbf': int(time.time()),
        'exp': int(time.time()) + 120,
        "uri": uri,
        "body": minified_body
    }
    jwt_token = build_jwt(jwt_payload)

    return jwt_token