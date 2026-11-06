import os

import pymysql
import requests
from dotenv import load_dotenv

# File .env berada di luar folder dags
load_dotenv("/opt/airflow/repo/.env")

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
}


def _require_env(*keys: str) -> None:
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise ValueError("Konfigurasi .env belum lengkap: " + ", ".join(missing))


def login() -> tuple[str, str]:
    _require_env("PLPBK_BASE_URL", "PLPBK_USERNAME", "PLPBK_PASSWORD")
    base_url = os.getenv("PLPBK_BASE_URL").rstrip("/")

    resp = requests.post(
        f"{base_url}/login",
        json={
            "username": os.getenv("PLPBK_USERNAME"),
            "password": os.getenv("PLPBK_PASSWORD"),
        },
        headers=HEADERS,
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Login PLPBK gagal: HTTP {resp.status_code} | {resp.text[:300]}")

    body = resp.json()
    token = None
    if isinstance(body, dict):
        data = body.get("data") if isinstance(body.get("data"), dict) else {}
        token = body.get("token") or body.get("access_token") or data.get("token") or data.get("access_token")
    if not token:
        raise ValueError("Login berhasil tetapi token tidak ditemukan dalam respons API.")
    return base_url, token


def get_json(path: str, params: dict | None = None):
    """Login lalu GET path. Token tidak keluar dari fungsi ini."""
    base_url, token = login()
    resp = requests.get(
        f"{base_url}/{path.lstrip('/')}",
        params=params,
        headers={**HEADERS, "Authorization": f"Bearer {token}"},
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"GET /{path.lstrip('/')} gagal: HTTP {resp.status_code} | {resp.text[:300]}")
    return resp.json()


def get_db_connection():
    _require_env("DB_NAME", "DB_USER", "DB_PASSWORD")
    try:
        return pymysql.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
    except pymysql.MySQLError as e:
        raise RuntimeError(f"Gagal konek MariaDB: {type(e).__name__} {e.args}") from None
