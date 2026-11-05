# ============================================================
# IMPORT LIBRARY
# ============================================================

# Mengimpor modul logging untuk mencatat aktivitas dan proses ETL Airflow.
import logging

# Mengimpor json untuk mengubah data Python menjadi format JSON.
import json

# Mengimpor os untuk membaca konfigurasi dari environment variable.
import os

# Mengimpor datetime untuk menentukan tanggal mulai DAG.
# Mengimpor timedelta untuk mengatur jeda waktu retry.
from datetime import datetime, timedelta

# Mengimpor requests untuk melakukan komunikasi HTTP dengan API SISTER.
import requests

# Mengimpor pymysql untuk menghubungkan Python dengan database MariaDB.
import pymysql

# Mengimpor load_dotenv untuk membaca konfigurasi dari file .env.
from dotenv import load_dotenv

# Mengimpor decorator dag dan task dari Airflow SDK.
from airflow.sdk import dag, task

# Mencoba mengimpor BaseHook dari Airflow SDK.
try:
    from airflow.sdk import BaseHook

# Jika tidak tersedia di airflow.sdk, gunakan modul hooks.base.
except ImportError:
    from airflow.hooks.base import BaseHook


# ============================================================
# KONFIGURASI LOGGING
# ============================================================

# Membuat objek logger dengan nama modul saat ini.
log = logging.getLogger(__name__)


# ============================================================
# KONFIGURASI FILE ENV
# ============================================================

# Menentukan lokasi file .env.
# File .env berada di luar folder dags.
ENV_FILE = "/opt/airflow/repo/.env"

# Membaca konfigurasi dari file .env.
load_dotenv(ENV_FILE)

# Mengambil konfigurasi database dari environment variable.
DB_HOST = os.getenv("DB_HOST", "localhost")

# Mengambil port database dan mengubahnya menjadi integer.
DB_PORT = int(os.getenv("DB_PORT", "3306"))

# Mengambil nama database.
DB_NAME = os.getenv("DB_NAME")

# Mengambil username database.
DB_USER = os.getenv("DB_USER")

# Mengambil password database.
DB_PASSWORD = os.getenv("DB_PASSWORD")


# ============================================================
# KONFIGURASI API SISTER
# ============================================================

# Menentukan ID SDM dosen yang akan digunakan
# sebagai parameter untuk mengambil data penelitian dari SISTER.
ID_SDM = "516381bf-1fce-4874-ae5f-a6b8cc24c5b0"

# Menentukan Connection ID Airflow untuk mengakses API SISTER.
# URL, username, password, dan id_pengguna disimpan
# pada Airflow Connection dengan ID sister_api.
SISTER_CONNECTION_ID = "sister_api"

# Membuat HTTP Header yang digunakan ketika mengakses API SISTER.
HEADERS = {
    # Memberitahukan bahwa aplikasi menerima respons JSON.
    "Accept": "application/json",

    # Mengidentifikasi aplikasi sebagai browser berbasis Linux.
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
}


# ============================================================
# FUNGSI LOGIN API SISTER
# ============================================================

def _login() -> tuple[str, str]:
    """
    Melakukan autentikasi ke API SISTER.

    Konfigurasi API diambil dari Airflow Connection:
    - Connection ID: sister_api
    - Login: username SISTER
    - Password: password SISTER
    - Extra: base_url dan id_pengguna

    Returns:
        tuple[str, str]:
            base_url dan token autentikasi.
    """

    # Mengambil konfigurasi koneksi SISTER dari Airflow.
    conn = BaseHook.get_connection(SISTER_CONNECTION_ID)

    # Mengambil konfigurasi tambahan dalam format JSON.
    extra = conn.extra_dejson

    # Mengambil alamat API SISTER.
    base_url = extra["base_url"].rstrip("/")

    # Mengirim HTTP POST ke endpoint authorize.
    resp = requests.post(
        f"{base_url}/authorize",

        # Mengirim username, password, dan ID pengguna.
        json={
            "username": conn.login,
            "password": conn.password,
            "id_pengguna": extra["id_pengguna"],
        },

        # Mengirim HTTP Header.
        headers=HEADERS,

        # Batas waktu permintaan maksimal 30 detik.
        timeout=30,
    )

    # Memeriksa apakah proses login berhasil.
    if not resp.ok:
        raise RuntimeError(
            f"Login SISTER gagal: HTTP {resp.status_code} | "
            f"{resp.text[:300]}"
        )

    # Mengambil token dari respons API.
    token = resp.json().get("token")

    # Memastikan token berhasil diperoleh.
    if not token:
        raise ValueError(
            "Login berhasil tetapi token tidak ditemukan dalam respons API."
        )

    # Mengembalikan alamat API dan token.
    return base_url, token


# ============================================================
# FUNGSI KONEKSI DATABASE MARIADB
# ============================================================

def _get_db_connection():
    """
    Membuat koneksi ke database MariaDB menggunakan PyMySQL.

    Konfigurasi database diambil dari file .env.
    """

    # Memastikan seluruh konfigurasi database sudah tersedia.
    required_config = {
        "DB_NAME": DB_NAME,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
    }

    # Mencari konfigurasi yang belum diisi.
    missing_config = [
        key
        for key, value in required_config.items()
        if not value
    ]

    # Menghentikan proses jika ada konfigurasi yang belum tersedia.
    if missing_config:
        raise ValueError(
            "Konfigurasi database belum lengkap: "
            + ", ".join(missing_config)
        )

    # Membuat dan mengembalikan koneksi MariaDB.
    return pymysql.connect(
        # Hostname database.
        host=DB_HOST,

        # Port database.
        port=DB_PORT,

        # Username database.
        user=DB_USER,

        # Password database.
        password=DB_PASSWORD,

        # Nama database.
        database=DB_NAME,

        # Menggunakan karakter UTF-8.
        charset="utf8mb4",

        # Mengatur hasil query dalam bentuk dictionary.
        cursorclass=pymysql.cursors.DictCursor,

        # Transaksi dikontrol secara manual.
        autocommit=False,
    )


# ============================================================
# DEFINISI DAG
# ============================================================

@dag(
    # Nama unik DAG di Airflow.
    dag_id="sister_penelitian",

    # Menjadwalkan DAG berjalan setiap hari pukul 02.00.
    schedule="0 2 * * *",

    # Tanggal mulai DAG.
    start_date=datetime(2026, 1, 1),

    # Tidak menjalankan jadwal masa lalu.
    catchup=False,

    # Konfigurasi default task.
    default_args={
        # Jumlah percobaan ulang jika task gagal.
        "retries": 2,

        # Jeda waktu sebelum melakukan retry.
        "retry_delay": timedelta(minutes=5),
    },

    # Tag untuk pengelompokan DAG di Airflow.
    tags=["sister", "etl", "mariadb"],
)

def sister_penelitian():

    # ========================================================
    # TASK ETL DATA PENELITIAN
    # ========================================================

    @task
    def tarik_dan_simpan_penelitian():

        # ----------------------------------------------------
        # 1. EXTRACT: LOGIN KE SISTER
        # ----------------------------------------------------

        # Mencatat informasi proses autentikasi.
        log.info("Memulai proses login ke SISTER.")

        # Melakukan login dan mengambil token.
        base_url, token = _login()

        # Mencatat keberhasilan login.
        log.info("Login SISTER berhasil.")


        # ----------------------------------------------------
        # 2. EXTRACT: AMBIL DATA PENELITIAN
        # ----------------------------------------------------

        # Mengirim HTTP GET untuk mengambil data penelitian.
        resp = requests.get(
            f"{base_url}/penelitian",

            # Mengirim ID SDM sebagai parameter pencarian.
            params={
                "id_sdm": ID_SDM
            },

            # Mengirim HTTP Header dan token autentikasi.
            headers={
                **HEADERS,
                "Authorization": f"Bearer {token}",
            },

            # Batas waktu permintaan maksimal 60 detik.
            timeout=60,
        )

        # Memeriksa apakah permintaan berhasil.
        if not resp.ok:
            raise RuntimeError(
                f"GET /penelitian gagal: HTTP {resp.status_code} | "
                f"{resp.text[:300]}"
            )

        # Mengubah respons JSON menjadi objek Python.
        data = resp.json()

        # Memastikan respons API berbentuk list.
        if not isinstance(data, list):
            raise ValueError(
                "Format respons API tidak sesuai. "
                "Respons yang diharapkan berupa JSON array."
            )

        # Menghitung jumlah data yang diperoleh.
        total_data = len(data)

        # Mencatat jumlah data dari API.
        log.info(
            "Berhasil mengambil %d data penelitian dari SISTER.",
            total_data,
        )

        # Memeriksa apakah terdapat data penelitian.
        if total_data == 0:
            log.info("Tidak ada data penelitian untuk disimpan.")
            return


        # ----------------------------------------------------
        # 3. TRANSFORM & LOAD: SIMPAN KE MARIADB
        # ----------------------------------------------------

        # Inisialisasi koneksi database.
        db = None

        # Penghitung jumlah data yang berhasil diproses.
        total_processed = 0

        # Penghitung data yang dilewati karena tidak valid.
        total_skipped = 0

        # Query untuk memasukkan atau memperbarui data penelitian.
        query = """
            INSERT INTO sister_penelitian (
                id,
                judul,
                tahun_pelaksanaan,
                lama_kegiatan,
                raw_response
            )
            VALUES (
                %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                judul = VALUES(judul),
                tahun_pelaksanaan = VALUES(tahun_pelaksanaan),
                lama_kegiatan = VALUES(lama_kegiatan),
                raw_response = VALUES(raw_response)
        """

        try:

            # Membuka koneksi database MariaDB.
            db = _get_db_connection()

            # Membuat cursor untuk menjalankan query SQL.
            with db.cursor() as cursor:

                # Memproses setiap data penelitian dari API.
                for row in data:

                    # Mengambil ID penelitian.
                    penelitian_id = row.get("id")

                    # Memastikan ID tersedia.
                    if not penelitian_id:
                        log.warning(
                            "Data penelitian dilewati karena tidak memiliki ID."
                        )

                        total_skipped += 1
                        continue

                    # Mengambil judul penelitian.
                    # Jika kosong, gunakan string kosong.
                    judul = row.get("judul") or ""

                    # Mengambil tahun pelaksanaan penelitian.
                    tahun = row.get("tahun_pelaksanaan")

                    # Mengambil lama kegiatan penelitian.
                    lama = row.get("lama_kegiatan")

                    # Mengubah tahun menjadi integer.
                    # Jika kosong, simpan sebagai NULL.
                    tahun = int(tahun) if tahun else None

                    # Mengubah lama kegiatan menjadi integer.
                    # Jika kosong, simpan sebagai NULL.
                    lama = int(lama) if lama is not None else None

                    # Mengubah seluruh data menjadi JSON.
                    # ensure_ascii=False mempertahankan karakter Unicode.
                    raw_response = json.dumps(
                        row,
                        ensure_ascii=False,
                    )

                    # Menjalankan query INSERT atau UPDATE.
                    cursor.execute(
                        query,
                        (
                            penelitian_id,
                            judul,
                            tahun,
                            lama,
                            raw_response,
                        ),
                    )

                    # Menambah jumlah data yang telah diproses.
                    total_processed += 1

            # Menyimpan perubahan ke database.
            db.commit()

            # Mencatat hasil proses ETL.
            log.info(
                "Proses ETL berhasil. "
                "Total data dari API: %d | "
                "Total diproses: %d | "
                "Total dilewati: %d",
                total_data,
                total_processed,
                total_skipped,
            )

        except Exception:

            # Jika terjadi kesalahan, batalkan transaksi.
            if db:
                db.rollback()

            # Mencatat detail kesalahan.
            log.exception(
                "Gagal menyimpan data penelitian ke MariaDB."
            )

            # Meneruskan exception agar Airflow dapat melakukan retry.
            raise

        finally:

            # Menutup koneksi database setelah proses selesai.
            if db:
                db.close()

    # ========================================================
    # EKSEKUSI PIPELINE ETL
    # ========================================================

    # Memanggil task ETL.
    tarik_dan_simpan_penelitian()


# ============================================================
# REGISTRASI DAG
# ============================================================

# Mendaftarkan DAG agar dapat ditemukan oleh Airflow Scheduler.
sister_penelitian()