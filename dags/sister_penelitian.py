# Mengimpor modul logging untuk mencatat aktivitas dan proses ETL Airflow.
import logging

# Mengimpor json untuk mengubah data Python menjadi format JSON.
import json

# Mengimpor datetime untuk menentukan tanggal mulai DAG.
# Mengimpor timedelta untuk mengatur jeda waktu retry.
from datetime import datetime, timedelta

# Mengimpor requests untuk melakukan komunikasi HTTP dengan API SISTER.
import requests

# Mengimpor pymysql untuk menghubungkan Python dengan database MariaDB.
import pymysql

# Mengimpor decorator dag dan task dari Airflow SDK.
# dag digunakan untuk mendefinisikan DAG.
# task digunakan untuk mendefinisikan task di dalam DAG.
from airflow.sdk import dag, task

# Mencoba mengimpor BaseHook dari Airflow SDK.
try:
    # Digunakan pada versi Airflow yang menyediakan BaseHook di airflow.sdk.
    from airflow.sdk import BaseHook

# Jika BaseHook tidak tersedia di airflow.sdk,
# gunakan lokasi modul hooks.base sebagai alternatif.
except ImportError:
    from airflow.hooks.base import BaseHook


# Membuat objek logger dengan nama modul saat ini.
# Logger digunakan untuk mencatat informasi, peringatan, dan kesalahan.
log = logging.getLogger(__name__)


# Menentukan ID SDM dosen yang akan digunakan
# sebagai parameter untuk mengambil data penelitian dari SISTER.
ID_SDM = "516381bf-1fce-4874-ae5f-a6b8cc24c5b0"


# Menentukan Connection ID Airflow untuk mengakses API SISTER.
# Konfigurasi URL, username, password, dan id_pengguna
# disimpan di Airflow Connection dengan ID sister_api.
SISTER_CONNECTION_ID = "sister_api"

# Menentukan Connection ID Airflow untuk mengakses database MariaDB.
# Host, port, database, username, dan password
# disimpan di Airflow Connection dengan ID sister_db.
DB_CONNECTION_ID = "sister_db"


# Membuat HTTP Header yang akan digunakan ketika mengakses API SISTER.
# SISTER memblokir User-Agent bawaan Python Requests sehingga
# digunakan User-Agent menyerupai browser.
HEADERS = {
    # Memberitahukan bahwa aplikasi menerima respons dalam format JSON.
    "Accept": "application/json",

    # Mengidentifikasi aplikasi sebagai browser berbasis Linux.
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
}


# Membuat fungsi untuk melakukan autentikasi ke API SISTER.
# Fungsi mengembalikan dua nilai:
# 1. base_url: alamat dasar API SISTER.
# 2. token: token autentikasi yang digunakan untuk mengakses API.
def _login() -> tuple[str, str]:

    # Mengambil konfigurasi koneksi SISTER dari Airflow Connection.
    conn = BaseHook.get_connection(SISTER_CONNECTION_ID)

    # Mengambil konfigurasi tambahan yang disimpan dalam format JSON.
    # Contohnya: base_url dan id_pengguna.
    extra = conn.extra_dejson

    # Mengambil alamat API SISTER dari konfigurasi tambahan.
    # rstrip("/") menghapus karakter slash di bagian akhir URL.
    base_url = extra["base_url"].rstrip("/")

    # Mengirim HTTP POST ke endpoint authorize untuk melakukan login.
    resp = requests.post(

        # Membentuk alamat endpoint autentikasi SISTER.
        f"{base_url}/authorize",

        # Mengirim username, password, dan ID pengguna dalam JSON.
        json={
            # Username diambil dari Airflow Connection.
            "username": conn.login,

            # Password diambil dari Airflow Connection.
            "password": conn.password,

            # ID pengguna diambil dari konfigurasi tambahan.
            "id_pengguna": extra["id_pengguna"],
        },

        # Mengirim HTTP Header yang telah didefinisikan sebelumnya.
        headers=HEADERS,

        # Menentukan batas waktu maksimal permintaan selama 30 detik.
        timeout=30,
    )

    # Memeriksa apakah proses login berhasil.
    # Jika HTTP status bukan 2xx, proses akan menghasilkan exception.
    if not resp.ok:

        # Menghentikan proses dan menampilkan status HTTP serta
        # maksimal 300 karakter pesan respons dari API.
        raise RuntimeError(
            f"Login SISTER gagal: HTTP {resp.status_code} | {resp.text[:300]}"
        )

    # Mengubah respons API menjadi JSON dan mengambil nilai token.
    token = resp.json().get("token")

    # Memeriksa apakah token berhasil diperoleh dari respons API.
    if not token:

        # Menghentikan proses jika token tidak ditemukan.
        raise ValueError(
            "Login berhasil tetapi token tidak ada di respons"
        )

    # Mengembalikan alamat API dan token untuk digunakan
    # pada proses pengambilan data penelitian.
    return base_url, token


# Membuat fungsi untuk membangun koneksi ke database MariaDB.
# Konfigurasi koneksi diambil dari Airflow Connection.
def _get_db_connection():

    # Mengambil konfigurasi koneksi database dari Airflow.
    conn = BaseHook.get_connection(DB_CONNECTION_ID)

    # Membuat dan mengembalikan koneksi MariaDB menggunakan PyMySQL.
    return pymysql.connect(

        # Menggunakan hostname database dari Airflow Connection.
        host=conn.host,

        # Menggunakan port database dari Airflow Connection.
        # Jika port tidak diisi, gunakan port default MariaDB 3306.
        port=conn.port or 3306,

        # Menggunakan username database dari Airflow Connection.
        user=conn.login,

        # Menggunakan password database dari Airflow Connection.
        password=conn.password,

        # Menggunakan nama database dari field Schema Airflow Connection.
        database=conn.schema,

        # Menggunakan karakter UTF-8 agar mendukung karakter Unicode.
        charset="utf8mb4",

        # Mengatur hasil query agar dapat diakses dalam bentuk dictionary.
        cursorclass=pymysql.cursors.DictCursor,

        # Menonaktifkan autocommit agar transaksi dapat dikontrol manual.
        autocommit=False,
    )


# Mendefinisikan DAG Airflow untuk proses ETL data penelitian SISTER.
@dag(

    # Menentukan ID atau nama unik DAG di Airflow.
    dag_id="sister_penelitian",

    # Menjadwalkan DAG berjalan setiap hari pukul 02.00.
    # Penentuan waktu mengikuti timezone yang dikonfigurasi di Airflow.
    schedule="0 2 * * *",

    # Menentukan tanggal mulai DAG aktif.
    start_date=datetime(2026, 1, 1),

    # Menonaktifkan eksekusi otomatis untuk jadwal masa lalu.
    catchup=False,

    # Menentukan konfigurasi default untuk seluruh task dalam DAG.
    default_args={

        # Jika task gagal, Airflow akan mencoba menjalankannya
        # kembali sebanyak dua kali.
        "retries": 2,

        # Memberikan jeda lima menit sebelum melakukan percobaan ulang.
        "retry_delay": timedelta(minutes=5),
    },

    # Memberikan tag untuk mempermudah pencarian dan pengelompokan DAG.
    tags=["sister", "etl", "mariadb"],
)

# Membuat fungsi utama DAG.
def sister_penelitian():

    # Mendefinisikan task untuk mengambil data dari SISTER
    # kemudian menyimpannya ke dalam database MariaDB.
    @task
    def tarik_dan_simpan_penelitian():

        # ==========================================
        # 1. EXTRACT: LOGIN KE SISTER
        # ==========================================

        # Mencatat informasi bahwa proses autentikasi akan dimulai.
        log.info("Memulai proses login ke SISTER")

        # Memanggil fungsi login untuk mendapatkan alamat API dan token.
        base_url, token = _login()

        # Mencatat bahwa proses autentikasi berhasil.
        log.info("Login SISTER berhasil")


        # ==========================================
        # 2. EXTRACT: AMBIL DATA PENELITIAN
        # ==========================================

        # Mengirim HTTP GET ke endpoint penelitian untuk mengambil data.
        resp = requests.get(

            # Membentuk URL endpoint penelitian.
            f"{base_url}/penelitian",

            # Mengirim ID SDM sebagai parameter pencarian.
            params={"id_sdm": ID_SDM},

            # Mengirim HTTP Header dan token autentikasi.
            headers={
                # Menyalin seluruh HTTP Header yang sudah didefinisikan.
                **HEADERS,

                # Menambahkan token autentikasi menggunakan Bearer Token.
                "Authorization": f"Bearer {token}",
            },

            # Menentukan batas waktu maksimal permintaan selama 60 detik.
            timeout=60,
        )

        # Memeriksa apakah permintaan pengambilan data berhasil.
        if not resp.ok:

            # Menghentikan proses jika API mengembalikan HTTP error.
            raise RuntimeError(
                f"GET /penelitian gagal: HTTP {resp.status_code} | "
                f"{resp.text[:300]}"
            )

        # Mengubah respons API dari format JSON menjadi objek Python.
        data = resp.json()

        # Memastikan bahwa respons API berbentuk list atau array.
        # Format ini diperlukan agar data dapat diproses menggunakan loop.
        if not isinstance(data, list):

            # Menghentikan proses jika format respons tidak sesuai.
            raise ValueError(
                "Format respons API tidak sesuai. "
                "Respons yang diharapkan berupa JSON array."
            )

        # Menghitung jumlah data penelitian yang berhasil diperoleh.
        total_data = len(data)

        # Mencatat jumlah data penelitian yang diperoleh dari API.
        log.info(
            "Berhasil mengambil %d data penelitian dari SISTER",
            total_data,
        )

        # Memeriksa apakah API mengembalikan data penelitian.
        if total_data == 0:

            # Mencatat informasi jika tidak ada data yang ditemukan.
            log.info("Tidak ada data penelitian untuk disimpan")

            # Menghentikan task karena tidak ada data yang perlu diproses.
            return


        # ==========================================
        # 3. TRANSFORM & LOAD: SIMPAN KE MARIADB
        # ==========================================

        # Menginisialisasi variabel koneksi database.
        # Nilai None menunjukkan bahwa koneksi belum dibuat.
        db = None

        # Menginisialisasi penghitung jumlah data yang diproses.
        total_insert = 0

        # Membuat perintah SQL untuk memasukkan data penelitian.
        query = """

            # Memasukkan data ke dalam tabel sister_penelitian.
            INSERT INTO sister_penelitian (

                # Kolom ID penelitian dari API SISTER.
                id,

                # Kolom judul penelitian.
                judul,

                # Kolom tahun pelaksanaan penelitian.
                tahun_pelaksanaan,

                # Kolom lama kegiatan penelitian.
                lama_kegiatan,

                # Kolom untuk menyimpan seluruh respons JSON asli.
                raw_response
            )

            # Menentukan lima parameter nilai yang akan dimasukkan.
            VALUES (
                %s, %s, %s, %s, %s
            )

            # Jika ID sudah ada karena merupakan Primary Key,
            # MariaDB akan memperbarui data yang sudah tersimpan.
            ON DUPLICATE KEY UPDATE

                # Memperbarui judul penelitian.
                judul = VALUES(judul),

                # Memperbarui tahun pelaksanaan.
                tahun_pelaksanaan = VALUES(tahun_pelaksanaan),

                # Memperbarui lama kegiatan.
                lama_kegiatan = VALUES(lama_kegiatan),

                # Memperbarui respons JSON asli.
                raw_response = VALUES(raw_response)
        """

        # Memulai blok penanganan kesalahan pada proses database.
        try:

            # Membuka koneksi ke database MariaDB.
            db = _get_db_connection()

            # Membuat cursor untuk menjalankan perintah SQL.
            # with memastikan cursor ditutup setelah blok selesai.
            with db.cursor() as cursor:

                # Melakukan perulangan untuk setiap data penelitian.
                for row in data:

                    # Mengambil ID penelitian dari setiap objek JSON.
                    penelitian_id = row.get("id")

                    # Memastikan setiap data penelitian memiliki ID.
                    if not penelitian_id:

                        # Mencatat peringatan jika ID penelitian tidak tersedia.
                        log.warning(
                            "Data penelitian dilewati karena tidak memiliki ID"
                        )

                        # Melewati data tersebut dan melanjutkan ke data berikutnya.
                        continue

                    # Mengambil judul penelitian.
                    # Jika judul kosong atau None, gunakan string kosong.
                    judul = row.get("judul") or ""

                    # Mengambil tahun pelaksanaan dari data penelitian.
                    tahun = row.get("tahun_pelaksanaan")

                    # Mengambil lama kegiatan dari data penelitian.
                    lama = row.get("lama_kegiatan")

                    # Mengubah tahun menjadi integer jika nilainya tersedia.
                    # Jika tidak tersedia, simpan sebagai NULL.
                    tahun = int(tahun) if tahun else None

                    # Mengubah lama kegiatan menjadi integer jika tersedia.
                    # Jika tidak tersedia, simpan sebagai NULL.
                    lama = int(lama) if lama is not None else None

                    # Mengubah seluruh objek data penelitian menjadi JSON.
                    # ensure_ascii=False mempertahankan karakter non-ASCII.
                    raw_response = json.dumps(
                        row,
                        ensure_ascii=False,
                    )

                    # Menjalankan perintah INSERT atau UPDATE ke MariaDB.
                    cursor.execute(

                        # Menggunakan query SQL yang telah dibuat sebelumnya.
                        query,

                        # Mengirim nilai parameter sesuai urutan kolom SQL.
                        (
                            penelitian_id,  # ID penelitian.
                            judul,           # Judul penelitian.
                            tahun,           # Tahun pelaksanaan.
                            lama,            # Lama kegiatan.
                            raw_response,    # Respons JSON asli.
                        ),
                    )

                    # Menambah penghitung setiap kali perintah SQL dijalankan.
                    total_insert += 1

            # Menyimpan seluruh perubahan transaksi ke database.
            db.commit()

            # Mencatat bahwa proses ETL berhasil diselesaikan.
            log.info(
                "Proses ETL berhasil. Total data diproses: %d",
                total_insert,
            )

        # Menangkap kesalahan yang terjadi selama proses penyimpanan.
        except Exception:

            # Memeriksa apakah koneksi database sudah berhasil dibuat.
            if db:

                # Membatalkan seluruh perubahan jika terjadi kesalahan.
                # Hal ini menjaga konsistensi transaksi database.
                db.rollback()

            # Mencatat detail kesalahan pada log Airflow.
            log.exception(
                "Gagal menyimpan data penelitian ke MariaDB"
            )

            # Meneruskan exception agar Airflow mengetahui task gagal.
            # Airflow akan menjalankan mekanisme retry jika masih tersedia.
            raise

        # Blok finally selalu dijalankan, baik proses berhasil maupun gagal.
        finally:

            # Memastikan koneksi database sudah berhasil dibuat.
            if db:

                # Menutup koneksi database setelah proses selesai.
                db.close()


    # ==========================================
    # 4. EKSEKUSI PIPELINE ETL
    # ==========================================

    # Memanggil task agar Airflow mendaftarkan dan menjalankannya.
    tarik_dan_simpan_penelitian()


# Mendaftarkan DAG ke Airflow agar dapat ditemukan scheduler.
sister_penelitian()