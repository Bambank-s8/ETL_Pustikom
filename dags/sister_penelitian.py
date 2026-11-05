import logging
from datetime import datetime, timedelta

import requests
from airflow.sdk import BaseHook, dag, task

log = logging.getLogger(__name__)

ID_SDM = "516381bf-1fce-4874-ae5f-a6b8cc24c5b0"


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
        headers={"Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
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
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            },
            timeout=60,
        )
        resp.raise_for_status()
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
EOF
