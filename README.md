# 🔍 OCR Certificate API

REST API berbasis **Flask + Python-doctr** untuk ekstraksi otomatis metadata sertifikat kapal dari file PDF/gambar menggunakan OCR. Cocok diintegrasikan dengan aplikasi Laravel untuk sinkronisasi data sertifikat secara otomatis.

---

## 📋 Fitur

- ✅ OCR dokumen PDF dan gambar (JPG, PNG, BMP, TIFF)
- ✅ Ekstraksi metadata sertifikat: `nosert`, `noreg`, `nmkpl`, `jenis_sert`, `tgl_berlaku`, dll.
- ✅ Validasi nomor sertifikat (cocokkan PDF vs data sistem)
- ✅ Penyimpanan otomatis ke PostgreSQL
- ✅ Endpoint sync untuk integrasi Laravel
- ✅ CORS diaktifkan (bisa diakses dari frontend/Laravel)
- ✅ Dukungan multi-sertifikat dalam satu dokumen

---

## 🗂️ Struktur Project

```
Py_Deploy/
├── app/
│   ├── app.py                    # Flask API server (entry point)
│   ├── main.py                   # CLI + Flask gabungan
│   ├── ocr_engine.py             # Mesin OCR (python-doctr)
│   ├── text_parser.py            # Parsing & ekstraksi metadata teks
│   ├── bbox_template_manager.py  # Manajemen template bbox
│   ├── bbox_template.py          # Logika ekstraksi per template
│   ├── db_writer.py              # Simpan hasil ke PostgreSQL
│   ├── database.py               # Koneksi SQLAlchemy
│   ├── config_loader.py          # Load config YAML
│   ├── config.yaml               # Konfigurasi OCR & parsing
│   ├── bbox_templates_config.yaml# Konfigurasi template bbox
│   ├── dataset_handler.py        # Handler batch dokumen
│   ├── app_context.py            # Konteks aplikasi
│   ├── init_db.py                # Inisialisasi tabel database
│   └── .env                      # Variabel environment (lokal)
├── requirements.txt
├── Dockerfile
├── docker-compose-staging.yml
└── docker-compose-prod.yml
```

---

## ⚙️ Konfigurasi Environment

Buat file `.env` di dalam folder `app/`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ocr_db
DB_USERNAME=postgres
DB_PASSWORD=your_password
```

> Jika sudah punya `DATABASE_URL` lengkap, bisa langsung ditambahkan:
> ```env
> DATABASE_URL=postgresql://postgres:password@localhost:5432/ocr_db
> ```

---

## 🚀 Cara Menjalankan (Lokal)

### 1. Aktivasi Virtual Environment

```powershell
# Windows
.\venv\Scripts\Activate.ps1
```

### 2. Install Dependencies

```powershell
pip install -r requirements.txt
```

### 3. Inisialisasi Database

```powershell
cd app
python init_db.py
```

### 4. Jalankan Server

```powershell
cd app
python app.py
```

Server berjalan di: **`http://127.0.0.1:5000`**

---

## 🐳 Cara Menjalankan (Docker)

### Staging

```bash
docker-compose -f docker-compose-staging.yml up -d --build
```

API akan tersedia di port **`5193`** → mapped ke `5000` di dalam container.

### Production

```bash
docker-compose -f docker-compose-prod.yml up -d --build
```

---

## 📡 API Endpoints

Base URL: `http://127.0.0.1:5000`

---

### `POST /api/certificate-ocr/upload`

Upload file PDF/gambar untuk diproses OCR.

**Request** — `multipart/form-data`:

| Field     | Type   | Wajib | Keterangan                               |
|-----------|--------|-------|------------------------------------------|
| `file`    | File   | ✅     | File PDF atau gambar                     |
| `nup`     | String | ❌     | NUP pengguna yang upload                 |
| `sign_no` | String | ❌     | Nomor agenda / tanda tangan              |
| `nosert`  | String | ❌     | Nomor sertifikat dari sistem (validasi)  |

**Contoh curl:**
```bash
curl -X POST http://127.0.0.1:5000/api/certificate-ocr/upload \
  -F "file=@sertifikat.pdf" \
  -F "nup=12345" \
  -F "sign_no=SN-001" \
  -F "nosert=32333"
```

**Response sukses:**
```json
{
  "status": "success",
  "session_id": "uuid-...",
  "metadata": {
    "nosert": "32333",
    "nmkpl": "WINDUK ARSA",
    "tgl_berlaku": "2025-12-31",
    ...
  }
}
```

---

### `GET /api/certificate-ocr/results`

Ambil semua hasil OCR dari database.

**Query Params:**

| Param    | Default | Keterangan           |
|----------|---------|----------------------|
| `limit`  | 10      | Jumlah data per page |
| `offset` | 0       | Skip N record        |

**Contoh:**
```bash
curl "http://127.0.0.1:5000/api/certificate-ocr/results?limit=20&offset=0"
```

---

### `GET /api/certificate-ocr/sync`

Sinkronisasi data OCR berdasarkan nomor sertifikat. Digunakan Laravel untuk mengambil `tgl_berlaku` dan validasi kecocokan PDF.

**Query Params:**

| Param    | Wajib | Keterangan            |
|----------|-------|-----------------------|
| `nosert` | ✅     | Nomor sertifikat      |

**Contoh:**
```bash
curl "http://127.0.0.1:5000/api/certificate-ocr/sync?nosert=32333"
```

**Response:**
```json
{
  "status": "success",
  "found": true,
  "validation": {
    "nosert_match": true,
    "validation_status": "match",
    "nosert_expected": "32333",
    "nosert_ocr": "32333",
    "message": "Nomor sertifikat pada PDF sesuai dengan data di sistem."
  },
  "data": {
    "nosert": "32333",
    "nmkpl": "WINDUK ARSA",
    "tgl_berlaku": "2025-12-31",
    ...
  }
}
```

**Status validasi:**

| `validation_status` | Arti                                              |
|---------------------|---------------------------------------------------|
| `match`             | Nomor sertifikat PDF cocok dengan sistem           |
| `mismatch`          | ⚠️ PDF yang diupload berbeda dari data sistem      |
| `skipped`           | Validasi dilewati (nosert tidak dikirim saat upload)|

---

### `GET /health`

Health check server.

```bash
curl http://127.0.0.1:5000/health
# {"status": "ok"}
```

---

## 🗄️ Skema Database

Tabel: `public.parsing_results`

| Kolom              | Tipe        | Keterangan                          |
|--------------------|-------------|-------------------------------------|
| `id`               | SERIAL PK   | Primary key                         |
| `nosert`           | VARCHAR     | Nomor sertifikat (final)            |
| `nosert_ocr`       | VARCHAR     | Nomor sertifikat hasil baca OCR     |
| `nosert_expected`  | VARCHAR     | Nomor sertifikat dari Laravel       |
| `noreg`            | VARCHAR     | Nomor registrasi kapal              |
| `nmkpl`            | VARCHAR     | Nama kapal                          |
| `jenis_sert`       | VARCHAR     | Jenis sertifikat (HULL, MACH, dll.) |
| `jenis_survey`     | VARCHAR     | Jenis survey                        |
| `divisi`           | VARCHAR     | Divisi                              |
| `lokasi_survey`    | VARCHAR     | Lokasi survey                       |
| `tgl_sert`         | VARCHAR     | Tanggal terbit sertifikat           |
| `tgl_berlaku`      | VARCHAR     | Tanggal berlaku                     |
| `tgl_survey1`      | VARCHAR     | Tanggal survey 1                    |
| `tgl_survey2`      | VARCHAR     | Tanggal survey 2                    |
| `nup`              | VARCHAR     | NUP pengguna yang upload            |
| `sign_no`          | VARCHAR     | Nomor tanda tangan                  |
| `raw_result`       | JSONB       | Raw hasil parsing JSON              |
| `created_at`       | TIMESTAMP   | Waktu dibuat                        |
| `updated_at`       | TIMESTAMP   | Waktu diupdate                      |

---

## 🔧 Dependencies Utama

| Library           | Kegunaan                        |
|-------------------|---------------------------------|
| `Flask`           | Web framework / REST API        |
| `flask-cors`      | Mengizinkan cross-origin request |
| `python-doctr`    | OCR engine deep learning        |
| `torch`           | Backend deep learning (CPU)     |
| `SQLAlchemy`      | ORM database                    |
| `psycopg2-binary` | Driver PostgreSQL                |
| `python-dotenv`   | Load environment variables      |
| `Pillow`          | Pemrosesan gambar               |
| `pdf2image`       | Konversi PDF ke gambar          |
| `PyYAML`          | Load file konfigurasi YAML      |

---

## ⚠️ Catatan

- Pastikan **PostgreSQL** sudah berjalan sebelum start server
- Jalankan **selalu dari folder `app/`** agar import module lokal bekerja dengan benar:
  ```powershell
  cd app && python app.py
  ```
- Untuk menjalankan dari root, gunakan:
  ```powershell
  C:\Py_Deploy\venv\Scripts\python.exe app\app.py
  ```
