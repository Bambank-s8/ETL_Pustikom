import os

import pymysql
import requests
from dotenv import load_dotenv

try:
    from airflow.sdk import BaseHook
except ImportError:  # Airflow 3.0.x
    from airflow.hooks.base import BaseHook

# File .env berada di luar folder dags
load_dotenv("/opt/airflow/repo/.env")

SISTER_CONNECTION_ID = "sister_api"

# SISTER memblokir User-Agent bawaan python-requests (HTTP 403)
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
}


def login() -> tuple[str, str]:
    """Login ke SISTER, return (base_url, token)."""
    conn = BaseHook.get_connection(SISTER_CONNECTION_ID)
    extra = conn.extra_dejson
    base_url = extra["base_url"].rstrip("/")

    resp = requests.post(
        f"{base_url}/authorize",
        json={
            "username": conn.login,
            "password": conn.password,
            "id_pengguna": extra["id_pengguna"],
        },
        headers=HEADERS,
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Login SISTER gagal: HTTP {resp.status_code} | {resp.text[:300]}")
    token = resp.json().get("token")
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
    """Koneksi MariaDB dari file .env (transaksi dikontrol manual)."""
    missing = [k for k in ("DB_NAME", "DB_USER", "DB_PASSWORD") if not os.getenv(k)]
    if missing:
        raise ValueError("Konfigurasi database belum lengkap: " + ", ".join(missing))

    # Nilai password sengaja tidak ditaruh di variabel lokal, supaya tidak
    # tercetak di traceback log task kalau koneksi gagal.
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