"""Worker server: melayani aplikasi LONG (id 1) dan SHORT (id 2)."""
import argparse
import os
import socketserver
import threading
import time

from common import APPS, DEFAULT_HOST, log


def run_long(payload, delay):
    """Simulasi perhitungan kompleks: hitung banyaknya bilangan prima < N + jeda."""
    n = int(payload) if payload.strip().isdigit() else 20000
    count = sum(1 for k in range(2, n)
                if all(k % d for d in range(2, int(k ** 0.5) + 1)))
    time.sleep(delay)
    return f"primes_below_{n}={count}"


def run_short(payload, delay):
    """Simulasi aplikasi ringan: echo."""
    time.sleep(delay)
    return f"echo:{payload}"


class WorkerHandler(socketserver.StreamRequestHandler):
    def handle(self):
        wid = self.server.worker_id
        tag = f"WORKER-{wid}"
        line = self.rfile.readline().decode("utf-8").strip()
        if not line:
            return
        parts = line.split(" ", 2)
        if parts[0] != "REQ" or len(parts) < 2 or not parts[1].isdigit() or int(parts[1]) not in APPS:
            log(tag, f"pesan tidak valid: {line!r}")
            self.wfile.write(b"RES ERR invalid_request\n")
            return
        app_id = int(parts[1])
        payload = parts[2] if len(parts) > 2 else ""
        log(tag, f"menerima request app={APPS[app_id]} payload={payload!r} (menunggu slot)")

        with self.server.slot:  # kapasitas worker dibatasi (default 1 = satu pekerjaan sekali waktu)
            t0 = time.time()
            log(tag, f"mulai memproses app={APPS[app_id]}")
            if app_id == 1:
                result = run_long(payload, self.server.long_delay)
            else:
                result = run_short(payload, self.server.short_delay)
            dt = time.time() - t0
        log(tag, f"selesai app={APPS[app_id]} dalam {dt:.2f}s -> {result}")
        self.wfile.write(f"RES OK {wid} {APPS[app_id]} {result}\n".encode("utf-8"))


class WorkerServer(socketserver.ThreadingTCPServer):
    # Di Windows SO_REUSEADDR membolehkan dua proses bind ke port yang sama -> matikan di sana
    allow_reuse_address = os.name != "nt"
    daemon_threads = True


def main():
    ap = argparse.ArgumentParser(description="Worker server")
    ap.add_argument("--id", type=int, required=True, help="server-id worker (1..3)")
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--capacity", type=int, default=1, help="jumlah request yang diproses bersamaan")
    ap.add_argument("--long-delay", type=float, default=1.0, help="durasi simulasi aplikasi LONG (detik)")
    ap.add_argument("--short-delay", type=float, default=0.05, help="durasi simulasi aplikasi SHORT (detik)")
    a = ap.parse_args()

    server = WorkerServer((a.host, a.port), WorkerHandler)
    server.worker_id = a.id
    server.slot = threading.Semaphore(a.capacity)
    server.long_delay = a.long_delay
    server.short_delay = a.short_delay
    log(f"WORKER-{a.id}", f"siap di {a.host}:{a.port} (capacity={a.capacity})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log(f"WORKER-{a.id}", "dihentikan")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
