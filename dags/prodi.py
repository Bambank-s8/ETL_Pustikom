import json
import logging
import os
from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task
from sister_common import HEADERS, get_db_connection

log = logging.getLogger(__name__)

# Endpoint SIAKAD (bukan SISTER, tidak perlu login). Bisa dioverride lewat .env
PRODI_URL = os.getenv(
    "SIAKAD_PRODI_URL",
    "http://192.168.4.30/siakad_api/api/as400/programStudi/All",
)

# Dibuat otomatis kalau belum ada
DDL = """
    CREATE TABLE IF NOT EXISTS prodi_unj (
        kode_prodi       VARCHAR(10)  NOT NULL PRIMARY KEY,
        nama_fakultas    VARCHAR(100) NOT NULL,
        nama_prodi       VARCHAR(150) NOT NULL,
        jenjang_prodi    VARCHAR(5)   NOT NULL,
        nama_koordprodi  VARCHAR(100) NULL,
        raw_response     JSON         NOT NULL,
        updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

# Insert baru, atau update jika kode_prodi sudah ada
QUERY = """
    INSERT INTO prodi_unj (
        kode_prodi, nama_fakultas, nama_prodi, jenjang_prodi,
        nama_koordprodi, raw_response
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        nama_fakultas = VALUES(nama_fakultas),
        nama_prodi = VALUES(nama_prodi),
        jenjang_prodi = VALUES(jenjang_prodi),
        nama_koordprodi = VALUES(nama_koordprodi),
        raw_response = VALUES(raw_response)
"""


def _clean(value):
    """Rapikan spasi, string kosong menjadi NULL."""
    value = (value or "").strip()
    return value or None


@dag(
    dag_id="prodi_unj",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["siakad", "etl", "mariadb"],
)
def prodi_unj():

    @task
    def tarik_dan_simpan_prodi():

        # 1. EXTRACT: GET daftar program studi dari SIAKAD
        resp = requests.get(PRODI_URL, headers=HEADERS, timeout=60)
        if not resp.ok:
            raise RuntimeError(f"GET prodi gagal: HTTP {resp.status_code} | {resp.text[:300]}")

        body = resp.json()
        if not isinstance(body, dict) or not body.get("status"):
            raise ValueError(f"Respons API tidak sukses: {str(body)[:300]}")

        data = body.get("isi")
        if not isinstance(data, list):
            raise ValueError("Format respons API tidak sesuai. Field 'isi' harus JSON array.")

        total_data = len(data)
        log.info("Berhasil mengambil %d data prodi dari SIAKAD.", total_data)
        if total_data == 0:
            log.info("Tidak ada data prodi untuk disimpan.")
            return

        # 2. TRANSFORM: ambil kolom yang dibutuhkan
        rows = []
        total_skipped = 0
        for row in data:
            kode_prodi = _clean(row.get("kodeProdi"))
            nama_prodi = _clean(row.get("namaProdi"))
            if not kode_prodi or not nama_prodi:
                log.warning("Data prodi dilewati karena kode/nama kosong: %s", row)
                total_skipped += 1
                continue

            rows.append((
                kode_prodi,
                _clean(row.get("namaFakultas")) or "",
                nama_prodi,
                _clean(row.get("jenjangProdi")) or "",
                _clean(row.get("namaKoordProdi")),
                json.dumps(row, ensure_ascii=False),
            ))

        if not rows:
            log.warning("Tidak ada baris valid untuk disimpan.")
            return

        # 3. LOAD: simpan ke MariaDB dalam satu transaksi
        db = None
        try:
            db = get_db_connection()
            with db.cursor() as cursor:
                cursor.execute(DDL)
                cursor.executemany(QUERY, rows)
            db.commit()
            log.info(
                "Proses ETL berhasil. Total dari API: %d | Disimpan: %d | Dilewati: %d",
                total_data, len(rows), total_skipped,
            )
        except Exception:
            if db:
                db.rollback()
            log.exception("Gagal menyimpan data prodi ke MariaDB.")
            raise
        finally:
            if db:
                db.close()

    tarik_dan_simpan_prodi()


prodi_unj()
