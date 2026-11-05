import logging
from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task

try:
    from airflow.sdk import BaseHook
except ImportError:  # Airflow 3.0.x
    from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

ID_SDM = "516381bf-1fce-4874-ae5f-a6b8cc24c5b0"

# SISTER memblokir User-Agent bawaan python-requests (HTTP 403)
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
}


def _login() -> tuple[str, str]:
    """Login ke SISTER, return (base_url, token)."""
    conn = BaseHook.get_connection("sister_api")
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
        raise ValueError("Login berhasil tetapi token tidak ada di respons")
    return base_url, token


@dag(
    dag_id="sister_penelitian",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["sister", "etl"],
)
def sister_penelitian():

    @task
    def ambil_penelitian() -> list[dict]:
        # Login dan GET dalam satu task, supaya token tidak disimpan di XCom
        base_url, token = _login()

        resp = requests.get(
            f"{base_url}/penelitian",
            params={"id_sdm": ID_SDM},
            headers={**HEADERS, "Authorization": f"Bearer {token}"},
            timeout=60,
        )
        if not resp.ok:
            raise RuntimeError(f"GET /penelitian gagal: HTTP {resp.status_code} | {resp.text[:300]}")
        data = resp.json()
        log.info("Jumlah penelitian: %d", len(data))
        return data

    @task
    def proses(data: list[dict]):
        for row in data:
            log.info(
                "%s | %s | tahun=%s | lama=%s",
                row.get("id"),
                row.get("judul"),
                row.get("tahun_pelaksanaan"),
                row.get("lama_kegiatan"),
            )

    proses(ambil_penelitian())


sister_penelitian()