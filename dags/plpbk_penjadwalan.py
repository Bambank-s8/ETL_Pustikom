import json
import logging
import os
from datetime import datetime, timedelta

import pymysql
import requests
from dotenv import load_dotenv

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

load_dotenv("/opt/airflow/repo/.env")

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
}

DDL = """
    CREATE TABLE IF NOT EXISTS plpbk_penjadwalan (
        id INT PRIMARY KEY,
        user_psikiater_id INT NULL,
        schedule_name VARCHAR(255) NULL,
        schedule_description TEXT NULL,
        schedule_kuota INT NULL,
        start_schedule DATETIME NULL,
        end_schedule DATETIME NULL,
        raw_response JSON NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) CHARACTER SET utf8mb4
"""

UPSERT = """
    INSERT INTO plpbk_penjadwalan (
        id, user_psikiater_id, schedule_name, schedule_description,
        schedule_kuota, start_schedule, end_schedule, raw_response
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        user_psikiater_id = VALUES(user_psikiater_id),
        schedule_name = VALUES(schedule_name),
        schedule_description = VALUES(schedule_description),
        schedule_kuota = VALUES(schedule_kuota),
        start_schedule = VALUES(start_schedule),
        end_schedule = VALUES(end_schedule),
        raw_response = VALUES(raw_response)
"""


def _require_env(*keys):
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise ValueError("Konfigurasi .env belum lengkap: " + ", ".join(missing))


def _to_int(value):
    return int(value) if value not in (None, "") else None


@dag(
    dag_id="plpbk_penjadwalan",
    schedule="0 2 * * *",
    start_date=datetime(2026, 11, 6),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["plpbk", "etl", "mariadb"],
)
def plpbk_penjadwalan():

    @task
    def tarik_dan_simpan_jadwal():
        # ---------------- LOGIN ----------------
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
            d = body.get("data") if isinstance(body.get("data"), dict) else {}
            token = body.get("token") or body.get("access_token") or d.get("token") or d.get("access_token")
        if not token:
            raise ValueError("Login berhasil tetapi token tidak ditemukan dalam respons API.")

        # ---------------- EXTRACT ----------------
        resp = requests.get(
            f"{base_url}/schedule-all-confirm",
            headers={**HEADERS, "Authorization": f"Bearer {token}"},
            timeout=60,
        )
        if not resp.ok:
            raise RuntimeError(f"GET /schedule-all-confirm gagal: HTTP {resp.status_code} | {resp.text[:300]}")

        data = resp.json()
        if isinstance(data, dict):
            data = data.get("data")
        if not isinstance(data, list):
            raise ValueError("Format respons tidak sesuai. Diharapkan JSON array (atau {'data': [...]}).")

        log.info("Berhasil mengambil %d jadwal dari PLPBK.", len(data))
        if not data:
            return

        # ---------------- TRANSFORM & LOAD ----------------
        _require_env("DB_NAME", "DB_USER", "DB_PASSWORD")
        db = None
        processed = skipped = 0
        try:
            # Password tidak ditaruh di variabel lokal agar tidak tercetak di traceback.
            try:
                db = pymysql.connect(
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

            with db.cursor() as cursor:
                cursor.execute(DDL)
                for row in data:
                    schedule_id = row.get("id") or row.get("schedule_id")
                    if not schedule_id:
                        log.warning("Jadwal dilewati karena tidak memiliki id.")
                        skipped += 1
                        continue

                    cursor.execute(
                        UPSERT,
                        (
                            schedule_id,
                            _to_int(row.get("user_psikiater_id")),
                            row.get("schedule_name"),
                            row.get("schedule_description"),
                            _to_int(row.get("schedule_kuota")),
                            row.get("start_schedule"),
                            row.get("end_schedule"),
                            json.dumps(row, ensure_ascii=False),
                        ),
                    )
                    processed += 1
            db.commit()
            log.info(
                "ETL berhasil. Dari API: %d | Diproses: %d | Dilewati: %d",
                len(data), processed, skipped,
            )
        except Exception:
            if db:
                db.rollback()
            log.exception("Gagal menyimpan data penjadwalan ke MariaDB.")
            raise
        finally:
            if db:
                db.close()

    tarik_dan_simpan_jadwal()


plpbk_penjadwalan()
