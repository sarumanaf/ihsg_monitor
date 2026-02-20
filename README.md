# 📈 IHSG Market Monitor & Algorithmic Sentiment Analysis

![Python](https://img.shields.io/badge/Python-3.12-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE.svg?logo=Apache%20Airflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-(Supabase)-4169E1.svg?logo=postgresql)
![Streamlit](https://img.shields.io/badge/Streamlit-1.37-FF4B4B.svg?logo=streamlit)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED.svg?logo=docker)

Sebuah *End-to-End Data Engineering Pipeline* untuk memantau pergerakan Indeks Harga Saham Gabungan (IHSG) dan tingkat likuiditas 4 Bank Besar di Indonesia (BBCA, BBRI, BMRI, BBNI). 

Proyek ini mengekstraksi data finansial secara otomatis setiap hari kerja, melakukan transformasi indikator teknikal, melakukan validasi kualitas data, menyimpannya ke Cloud Database, dan memvisualisasikannya melalui Dashboard interaktif.

🔗 **[Lihat Dashboard Live di Sini](https://ihsgmonitor.streamlit.app)**

---

## 🏗️ Arsitektur Data (ETL Pipeline)

Proyek ini menggunakan **Apache Airflow** yang berjalan di dalam **Docker** sebagai orkestrator utama. Pipeline dijalankan menggunakan `TaskFlow API` dengan arsitektur *8-Task Branching* sebagai berikut:

1. **`extract_ihsg` & `extract_top_stocks` (Paralel):** Menarik data historis 6 bulan terakhir dari Yahoo Finance API.
2. **`transform_data`:** Menggabungkan data, menghitung likuiditas total (*Top 4 Volume*), serta mengkalkulasi indikator teknikal *Moving Average 50* (MA50) dan *Relative Strength Index* (RSI).
3. **`check_data_quality`:** Melakukan *Data Quality Assertion* untuk memastikan tidak ada nilai *Null* atau harga minus sebelum masuk ke *database*.
4. **`cek_hari_bursa` (Branching):** Memeriksa hari eksekusi. Jika akhir pekan (*weekend*), pipeline akan beralih ke task `skip_load`.
5. **`load_to_supabase`:** Melakukan *Full Load* (Metode TRUNCATE & APPEND) ke database PostgreSQL (Supabase) secara aman menggunakan SQLAlchemy `engine.begin()`.
6. **`send_telegram_alert`:** Mengirimkan laporan status (Sukses/Gagal/Skipped) secara *real-time* ke bot Telegram *developer*.

---

## 📊 Dashboard Visualisasi (Front-End)

Visualisasi dibangun menggunakan **Streamlit** dan **Plotly**, menampilkan metrik bergaya *Modern Quant* dengan tema *Dark Mode*, meliputi:
- **Scorecard Metrik:** Perubahan harga IHSG, Status Tren, dan Volume.
- **Neon Area Chart:** Pergerakan harga IHSG yang berpotongan dengan batas tren jangka menengah (MA50).
- **Dynamic Volume Bar:** Grafik volume transaksi yang berubah warna (Hijau/Merah) menyesuaikan pergerakan IHSG harian.
- **RSI Gauge Chart:** Spidometer indikator momentum yang memetakan zona *Oversold* (Murah) dan *Overbought* (Mahal).

---

## ⚙️ Cara Menjalankan Secara Lokal (Local Setup)

Jika Anda ingin menjalankan proyek ini di mesin lokal Anda:

1. **Clone repository ini:**
   ```bash
   git clone https://github.com/sarumanaf/ihsg_monitor.git
   cd ihsg_monitor
   ```

2. **Setup Environment Variables:**
   Buat file `.env` di folder root dan isi dengan kredensial Anda:
   ```env
   SUPABASE_URI=postgresql://postgres:[PASSWORD_ANDA]@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres
   ```
   *Catatan: File `.env` sudah diabaikan melalui `.gitignore` demi keamanan.*

3. **Inisialisasi & Jalankan Apache Airflow via Docker:**
   Pertama, lakukan inisialisasi database Airflow (hanya dilakukan sekali):
   ```bash
   docker compose up airflow-init
   ```
   Setelah proses inisialisasi selesai (muncul pesan sukses/exited 0), jalankan seluruh container di background:
   ```bash
   docker compose up -d
   ```
   Akses Airflow UI di `http://localhost:8080` (Username: `airflow`, Password: `airflow`).

4. **Jalankan Streamlit Dashboard:**
   Siapkan file `.streamlit/secrets.toml` yang berisi `SUPABASE_URI`, lalu jalankan:
   ```bash
   streamlit run streamlit_app/main.py
   ```

---
*Proyek ini dibangun sebagai Latihan Project Data Engineering untuk mendemonstrasikan kemampuan orkestrasi, data quality, penguasaan database, dan visualisasi data.*