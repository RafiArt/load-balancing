# Tugas 1 — Socket untuk Cluster Computing

Mata kuliah: **Komputasi Berbasis Jaringan (AKSO) — Ganjil 2024**
Topik: Implementasi client-server socket yang mensimulasikan **load balancing** sederhana dengan arsitektur **broker–worker**.

---

## Daftar Isi
1. [Ringkasan Tugas](#1-ringkasan-tugas)
2. [Struktur Proyek](#2-struktur-proyek)
3. [Arsitektur & Alur Pesan](#3-arsitektur--alur-pesan)
4. [Protokol / Messaging Format](#4-protokol--messaging-format)
5. [Cara Menjalankan](#5-cara-menjalankan)
6. [Jawaban Terhadap Soal (poin per poin)](#6-jawaban-terhadap-soal-poin-per-poin)
7. [Uji Ketahanan](#7-uji-ketahanan)
8. [Hasil Simulasi](#8-hasil-simulasi)

---

## 1. Ringkasan Tugas

Membuat implementasi client-server socket dengan **broker** (server utama) yang mendistribusikan request dari client ke **3 worker server** dalam satu cluster. Terdapat **2 strategi alokasi**:
1. **Balanced** — pemerataan jumlah request yang sudah dilayani tiap worker.
2. **Sequential** — berurutan sesuai `server-id` berdasar counter milik broker (round-robin).

Setiap worker melayani **2 jenis aplikasi**:
- **LONG** (Application ID = 1) → perhitungan kompleks (hitung bilangan prima).
- **SHORT** (Application ID = 2) → echo / perhitungan sederhana.

Client mengirim pesan `REQ` dengan menyebutkan Application ID. Broker dan worker mencetak informasi aktivitas ke layar.

---

## 2. Struktur Proyek

```
files (1)/
├── broker.py              # Server utama (load balancer)
├── worker.py              # Worker server (ID 1..3)
├── client.py              # Client pengirim request
├── common.py              # Definisi protokol, APPS, fungsi log
├── simulate.py            # Skrip otomatis: spawn semua proses + skenario
├── resilience_test.py     # Uji ketahanan (failure, recovery, beban)
├── Laporan_Tugas1_Socket_Cluster.docx
├── logs/                  # Output simulate.py per komponen
└── logs_resilience/       # Output resilience_test.py
```

### Deskripsi file utama

| File | Peran | Komponen inti |
|---|---|---|
| [common.py](common.py) | Konstanta & utilitas bersama | `APPS = {1: "LONG", 2: "SHORT"}`, `log()`, `parse_app()` |
| [broker.py](broker.py) | Load balancer | `BrokerState` (lock + counter), `BrokerHandler`, strategi `balanced`/`sequential` |
| [worker.py](worker.py) | Pelayan request | `run_long()`, `run_short()`, `WorkerHandler`, kapasitas via `Semaphore` |
| [client.py](client.py) | Pengirim request | Multi-thread, dukung pola `--apps long,short,short`, ringkasan latency |
| [simulate.py](simulate.py) | Orkestrasi | Spawn 3 worker + broker + client untuk tiap kombinasi (strategi × pola) |
| [resilience_test.py](resilience_test.py) | Uji failure | 5 skenario: invalid, 1-down, all-down, concurrent, recovery |

---

## 3. Arsitektur & Alur Pesan

```
   ┌────────┐   REQ   ┌────────┐   REQ    ┌──────────┐
   │ Client │ ──────► │ Broker │ ───────► │ Worker-1 │
   └────────┘         │ :5000  │          └──────────┘
       ▲              │        │   REQ    ┌──────────┐
       │   RES        │        │ ───────► │ Worker-2 │
       └──────────────┤        │          └──────────┘
                      │        │   REQ    ┌──────────┐
                      │        │ ───────► │ Worker-3 │
                      └────────┘          └──────────┘
```

- **TCP socket** berbasis `socketserver.ThreadingTCPServer` → tiap koneksi ditangani thread terpisah.
- **Broker memilih worker** berdasarkan strategi, lalu **membuka koneksi TCP baru** ke worker tersebut dan meneruskan request.
- **Jawaban worker** dilewatkan broker apa adanya ke client.
- **State broker** (`served`, `inflight`, `rr_counter`) dilindungi `threading.Lock` agar aman concurrent.

---

## 4. Protokol / Messaging Format

Protokol teks sederhana, **satu pesan per baris**, diakhiri `\n`.

### Permintaan
```
Client -> Broker : REQ <app_id> <payload>
Broker -> Worker : REQ <app_id> <payload>        (diteruskan apa adanya)
```
- `app_id` ∈ {1, 2}  (1=LONG, 2=SHORT)
- `payload` bebas (untuk LONG: angka N batas atas bilangan prima; untuk SHORT: string apa saja)

### Jawaban
```
Worker -> Broker : RES OK <worker_id> <app_name> <hasil>
Broker -> Client : RES OK <worker_id> <app_name> <hasil>   (diteruskan)
Kesalahan        : RES ERR <alasan>
```

### Debug / monitoring
```
Client -> Broker : STATS
Broker -> Client : STATS served=a,b,c inflight=x,y,z
```

Contoh:
```
> REQ 1 50000
< RES OK 1 LONG primes_below_50000=5133

> REQ 2 hello
< RES OK 2 SHORT echo:hello

> REQ 9 abc
< RES ERR invalid_request (format: REQ <app_id 1|2> <payload>)
```

---

## 5. Cara Menjalankan

**Prasyarat:** Python 3.8+ (tanpa library eksternal, hanya stdlib).

### Opsi A — Simulasi otomatis (RECOMMENDED untuk pengumpulan)
Satu perintah menjalankan semua skenario (strategi × pola) secara otomatis:
```bash
python simulate.py
```
Output per komponen ditulis ke [logs/](logs/).

### Opsi B — Uji ketahanan
```bash
python resilience_test.py
```
Output tersimpan di [logs_resilience/](logs_resilience/).

### Opsi C — Manual (5 terminal terpisah)
Terminal 1–3: nyalakan 3 worker
```bash
python worker.py --id 1 --port 6001
python worker.py --id 2 --port 6002
python worker.py --id 3 --port 6003
```
Terminal 4: nyalakan broker (pilih strategi)
```bash
python broker.py --port 5000 --strategy balanced
# atau
python broker.py --port 5000 --strategy sequential
```
Terminal 5: jalankan client
```bash
python client.py --port 5000 --apps long,short,short --count 9 --stats
```

> **Catatan:**
> - Alamat default memakai `127.0.0.1` (bukan `localhost`). Di Windows, `localhost` bisa di-resolve ke IPv6 (`::1`) lebih dulu padahal server hanya listen di IPv4, sehingga tiap koneksi tertunda ±2 detik (latency membengkak ±4 detik karena ada dua hop).
> - Pastikan tidak ada broker/worker lama yang masih berjalan sebelum menjalankan simulasi, supaya client tidak terhubung ke proses lama. `simulate.py` akan berhenti dengan pesan jika port 5000 atau 6001–6003 sudah terpakai.

### Parameter penting

**broker.py**
| Flag | Default | Keterangan |
|---|---|---|
| `--host` | 127.0.0.1 | Alamat bind |
| `--port` | 5000 | Port broker |
| `--workers` | `127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003` | Daftar worker |
| `--strategy` | `balanced` | `balanced` atau `sequential` |

**worker.py**
| Flag | Default | Keterangan |
|---|---|---|
| `--id` | wajib | Server-id (1..3) |
| `--port` | wajib | Port listen |
| `--capacity` | 1 | Pekerjaan paralel per worker |
| `--long-delay` | 1.0 | Jeda simulasi LONG (detik) |
| `--short-delay` | 0.05 | Jeda simulasi SHORT (detik) |

**client.py**
| Flag | Default | Keterangan |
|---|---|---|
| `--apps` | `short` | Pola aplikasi, mis. `long,short,short` |
| `--payload` | `hello` | Isi request |
| `--count` | 1 | Jumlah request total |
| `--delay` | 0.1 | Jeda antar kirim (detik) |
| `--stats` | off | Cetak STATS broker di akhir |

---

## 6. Jawaban Terhadap Soal (poin per poin)

### Poin 1 — Broker menerima request & distribusi ke 3 worker
Diimplementasikan di [broker.py](broker.py). `BrokerHandler.handle()` membaca satu baris `REQ`, memanggil `state.choose()` untuk memilih worker, lalu meneruskannya via koneksi TCP baru ke `(host, port)` worker terpilih. Jawaban worker dilewatkan ke client.

### Poin 2 — Dua pendekatan alokasi

**2.1 Pemerataan jumlah request terlayani** (`strategy=balanced`), [broker.py:34-36](broker.py#L34-L36):
```python
idx = min(range(n), key=lambda i: (self.served[i], self.inflight[i], i))
```
Broker memilih worker dengan jumlah `served` terkecil. Jika seri, lihat `inflight` (yang sedang paling sedikit mengerjakan); jika seri juga, pilih `server-id` terkecil.

**2.2 Berurutan sesuai server-id dengan counter broker** (`strategy=sequential`), [broker.py:29-32](broker.py#L29-L32):
```python
idx = self.rr_counter % n
self.rr_counter += 1
```
Counter `rr_counter` naik tiap request, menghasilkan urutan deterministik: W1, W2, W3, W1, W2, W3, …

### Poin 3 — Dua jenis aplikasi per worker

**LONG (id=1)** — perhitungan kompleks, [worker.py:11-17](worker.py#L11-L17):
```python
n = int(payload) if payload.strip().isdigit() else 20000
count = sum(1 for k in range(2, n)
            if all(k % d for d in range(2, int(k ** 0.5) + 1)))
```
Hitung banyaknya bilangan prima < N. Hasil: `primes_below_<n>=<count>`.

**SHORT (id=2)** — echo sederhana, [worker.py:20-23](worker.py#L20-L23):
```python
return f"echo:{payload}"
```

### Poin 4 — Client kirim request dengan Application ID
Format pesan client: `REQ <app_id> <payload>\n`. Lihat [client.py:10-13](client.py#L10-L13):
```python
s.sendall(f"REQ {app_id} {payload}\n".encode("utf-8"))
```
Protokol lengkap didefinisikan di [common.py:12-18](common.py#L12-L18) dan ringkas di [bagian 4](#4-protokol--messaging-format) di atas.

### Poin 5 — Broker & worker cetak informasi ke layar
Fungsi `log(tag, msg)` di [common.py:21-24](common.py#L21-L24) mencetak stempel waktu `HH:MM:SS.mmm`. Dipakai broker ([broker.py:74-75](broker.py#L74-L75), [broker.py:90-91](broker.py#L90-L91)) dan worker ([worker.py:40](worker.py#L40), [worker.py:44](worker.py#L44), [worker.py:50](worker.py#L50)) untuk mencatat: siap listen, terima request, mulai memproses, selesai + durasi.

Contoh log broker:
```
[01:19:26.927] [BROKER] request #1 dari 127.0.0.1:56078 app=SHORT -> WORKER-1  [strategi=sequential, served=[0, 0, 0], inflight=[1, 0, 0]]
[01:19:26.979] [BROKER] request #1 selesai oleh WORKER-1 dalam 0.05s (total served=[1, 0, 0])
```

---

## 7. Uji Ketahanan

Dijalankan via `python resilience_test.py`. Lima skenario:

| # | Skenario | Hasil |
|---|---|---|
| 1 | Invalid request (app id salah, format rusak) | Broker balas `RES ERR invalid_request (format: ...)`, tidak crash |
| 2 | 1 worker (W2) dimatikan di tengah operasi | Broker tetap jalan, request ke W2 balas error jelas, W1 & W3 normal |
| 3 | Semua worker mati | Broker tetap melayani, tiap request dapat `RES ERR worker-N tidak dapat dihubungi` |
| 4 | 50 request paralel (beban concurrent) | 50/50 OK, distribusi merata (17/17/16) |
| 5 | Worker mati lalu dihidupkan lagi | Setelah recovery, worker langsung dipakai kembali tanpa restart broker |

---

## 8. Hasil Simulasi

Hasil `python simulate.py` (dengan `--long-delay=1.0`, `--short-delay=0.05`, 9 request):

### A. Pola uniform `short` (semua SHORT)
Baik `sequential` maupun `balanced` → **served = [3, 3, 3]** (seragam).

### B. Pola campuran `long,short,short`
```
sequential: ringkasan W1=1.77s, W2=0.05s, W3=0.05s | served=[3,3,3]
balanced:   ringkasan W1=1.95s, W2=0.05s, W3=0.05s | served=[7,1,1]
```
Dengan pola `long,short,short` (periode 3 = jumlah worker), strategi `sequential` menaruh **semua LONG di W1**: jumlah request tetap 3-3-3 tetapi beban komputasi timpang. Pada strategi `balanced`, W1 yang sibuk mengerjakan LONG belum menyelesaikan request apa pun sehingga `served`-nya tertahan, lalu broker terus memilih W1 (distribusi 7/1/1) dan SHORT ikut mengantre di belakang LONG.

### Observasi penting
- **Strategi sequential** memberi distribusi *jumlah request* yang merata, tapi tidak peduli berat/ringan tiap request.
- **Strategi balanced** menyeimbangkan jumlah request yang *sudah selesai*; pada beban heterogen (mix LONG & SHORT), worker yang sedang sibuk dengan pekerjaan berat justru tampak "paling sedikit melayani" sehingga terus dipilih (indikator tertinggal).
- Keduanya cocok untuk tujuan edukatif menunjukkan trade-off strategi load balancing.

---

**Author:**
<u>**MUHAMMAD AINURROFIQ ANWAR BUHANG**</u>
NIM: 26051905007
<u>**MEAKHEL GUNAWAN**</u>
NIM: 26051905019
