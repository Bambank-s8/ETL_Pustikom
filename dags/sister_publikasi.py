import json
import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from sister_common import get_db_connection, get_json

log = logging.getLogger(__name__)

# ID SDM dosen yang datanya diambil dari SISTER
ID_SDM = "516381bf-1fce-4874-ae5f-a6b8cc24c5b0"

# Insert baru, atau update jika id sudah ada
QUERY = """
    INSERT INTO sister_publikasi (
        id, id_sdm, kategori_kegiatan, judul, quartile,
        bidang_keilmuan, jenis_publikasi, tanggal, asal_data, raw_response
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        id_sdm = VALUES(id_sdm),
        kategori_kegiatan = VALUES(kategori_kegiatan),
        judul = VALUES(judul),
        quartile = VALUES(quartile),
        bidang_keilmuan = VALUES(bidang_keilmuan),
        jenis_publikasi = VALUES(jenis_publikasi),
        tanggal = VALUES(tanggal),
        asal_data = VALUES(asal_data),
        raw_response = VALUES(raw_response)
"""


def _to_int(value):
    """Ubah ke integer, kosong atau tidak valid menjadi NULL."""
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _to_date(value):
    """Ubah teks tanggal (YYYY-MM-DD) menjadi date, gagal menjadi NULL."""
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


@dag(
    dag_id="sister_publikasi",
    schedule="30 2 * * *",  # 30 menit setelah sister_penelitian (UTC)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["sister", "etl", "mariadb"],
)
def sister_publikasi():

    @task
    def tarik_dan_simpan_publikasi():

        # 1. EXTRACT: login lalu GET /publikasi
        data = get_json("publikasi", {"id_sdm": ID_SDM})

        if not isinstance(data, list):
            raise ValueError("Format respons API tidak sesuai. Diharapkan JSON array.")

        total_data = len(data)
        log.info("Berhasil mengambil %d data publikasi dari SISTER.", total_data)
        if total_data == 0:
            log.info("Tidak ada data publikasi untuk disimpan.")
            return

        # 2. TRANSFORM: rapikan tipe data tiap baris
        rows = []
        total_skipped = 0
        for row in data:
            publikasi_id = row.get("id")
            if not publikasi_id:
                log.warning("Data publikasi dilewati karena tidak memiliki ID.")
                total_skipped += 1
                continue

            tanggal = _to_date(row.get("tanggal"))
            if row.get("tanggal") and tanggal is None:
                log.warning("Format tanggal tidak dikenali (id=%s), disimpan NULL.", publikasi_id)

            rows.append((
                publikasi_id,
                ID_SDM,
                row.get("kategori_kegiatan"),
                row.get("judul") or "",
                _to_int(row.get("quartile")),
                json.dumps(row.get("bidang_keilmuan") or [], ensure_ascii=False),
                row.get("jenis_publikasi"),
                tanggal,
                row.get("asal_data"),
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
                cursor.executemany(QUERY, rows)
            db.commit()
            log.info(
                "Proses ETL berhasil. Total dari API: %d | Disimpan: %d | Dilewati: %d",
                total_data, len(rows), total_skipped,
            )
        except Exception:
            if db:
                db.rollback()
            log.exception("Gagal menyimpan data publikasi ke MariaDB.")
            raise
        finally:
            if db:
                db.close()

    tarik_dan_simpan_publikasi()


sister_publikasi()