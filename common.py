"""Definisi bersama: ID aplikasi, format pesan, dan fungsi log."""
import time

# Pakai alamat IP numerik (bukan "localhost"): di Windows "localhost" bisa di-resolve ke IPv6 (::1)
# lebih dulu, sedangkan server hanya listen di IPv4 -> tiap koneksi tertunda ~2 detik.
DEFAULT_HOST = "127.0.0.1"

# Application ID yang dilayani setiap worker
APPS = {1: "LONG", 2: "SHORT"}
NAME_TO_ID = {"1": 1, "2": 2, "LONG": 1, "SHORT": 2}

# ---------------- Protokol (teks, satu pesan per baris, diakhiri '\n') ----------------
# Client -> Broker : REQ <app_id> <payload>
# Broker -> Worker : REQ <app_id> <payload>              (diteruskan apa adanya)
# Worker -> Broker : RES OK <worker_id> <app_name> <hasil>
# Broker -> Client : RES OK <worker_id> <app_name> <hasil>   (diteruskan)
# Kesalahan        : RES ERR <alasan>
# Client -> Broker : STATS  ->  STATS served=a,b,c inflight=x,y,z


def log(tag, msg):
    now = time.time()
    stamp = time.strftime("%H:%M:%S", time.localtime(now)) + f".{int(now * 1000) % 1000:03d}"
    print(f"[{stamp}] [{tag}] {msg}", flush=True)


def parse_app(token):
    """Ubah '1'/'2'/'long'/'short' menjadi application id; None bila tidak valid."""
    return NAME_TO_ID.get(str(token).strip().upper())
