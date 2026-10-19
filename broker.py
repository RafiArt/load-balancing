"""Broker: menerima request client dan mendistribusikannya ke worker."""
import argparse
import os
import socket
import socketserver
import threading
import time

from common import APPS, DEFAULT_HOST, log

STRATEGIES = ("balanced", "sequential")


class BrokerState:
    """Status broker (dilindungi lock karena diakses banyak thread)."""

    def __init__(self, workers, strategy):
        self.workers = workers              # list (host, port); indeks 0 -> worker-1
        self.strategy = strategy
        self.lock = threading.Lock()
        self.served = [0] * len(workers)    # request yang SUDAH selesai dilayani
        self.inflight = [0] * len(workers)  # request yang sedang dikerjakan
        self.rr_counter = 0                 # counter untuk alokasi berurutan
        self.seq = 0                        # nomor request global

    def choose(self):
        with self.lock:
            n = len(self.workers)
            if self.strategy == "sequential":
                # Pendekatan 2: berurutan sesuai server-id berdasar counter broker
                idx = self.rr_counter % n
                self.rr_counter += 1
            else:
                # Pendekatan 1: pemerataan jumlah request yang sudah dilayani
                # (tie-break: yang sedang paling sedikit dikerjakan, lalu server-id terkecil)
                idx = min(range(n), key=lambda i: (self.served[i], self.inflight[i], i))
            self.inflight[idx] += 1
            self.seq += 1
            return idx, self.seq

    def finish(self, idx, ok):
        with self.lock:
            self.inflight[idx] -= 1
            if ok:
                self.served[idx] += 1

    def snapshot(self):
        with self.lock:
            return list(self.served), list(self.inflight)


class BrokerHandler(socketserver.StreamRequestHandler):
    def handle(self):
        state = self.server.state
        line = self.rfile.readline().decode("utf-8").strip()
        if not line:
            return

        if line == "STATS":
            served, inflight = state.snapshot()
            self.wfile.write(("STATS served=%s inflight=%s\n" % (
                ",".join(map(str, served)), ",".join(map(str, inflight)))).encode())
            return

        parts = line.split(" ", 2)
        if parts[0] != "REQ" or len(parts) < 2 or not parts[1].isdigit() or int(parts[1]) not in APPS:
            log("BROKER", f"request tidak valid dari {self.client_address}: {line!r}")
            self.wfile.write(b"RES ERR invalid_request (format: REQ <app_id 1|2> <payload>)\n")
            return
        app_name = APPS[int(parts[1])]

        idx, seq = state.choose()
        served, inflight = state.snapshot()
        log("BROKER", f"request #{seq} dari {self.client_address[0]}:{self.client_address[1]} "
                      f"app={app_name} -> WORKER-{idx + 1}  "
                      f"[strategi={state.strategy}, served={served}, inflight={inflight}]")
        t0 = time.time()
        ok = False
        try:
            host, port = state.workers[idx]
            with socket.create_connection((host, port), timeout=60) as s:
                s.sendall((line + "\n").encode("utf-8"))
                reply = s.makefile("r", encoding="utf-8").readline().strip()
            ok = reply.startswith("RES OK")
        except OSError as e:
            reply = f"RES ERR worker-{idx + 1} tidak dapat dihubungi ({e})"
        finally:
            state.finish(idx, ok)
        served, _ = state.snapshot()
        log("BROKER", f"request #{seq} selesai oleh WORKER-{idx + 1} dalam {time.time() - t0:.2f}s "
                      f"(total served={served})")
        self.wfile.write((reply + "\n").encode("utf-8"))


class BrokerServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = os.name != "nt"  # lihat catatan di worker.py
    daemon_threads = True


def parse_workers(text):
    out = []
    for item in text.split(","):
        host, port = item.rsplit(":", 1)
        out.append((host, int(port)))
    return out


def main():
    ap = argparse.ArgumentParser(description="Broker load balancer sederhana")
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--workers", default="127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003")
    ap.add_argument("--strategy", choices=STRATEGIES, default="balanced",
                    help="balanced = pemerataan jumlah request terlayani; sequential = berurutan (round robin)")
    a = ap.parse_args()

    state = BrokerState(parse_workers(a.workers), a.strategy)
    server = BrokerServer((a.host, a.port), BrokerHandler)
    server.state = state
    log("BROKER", f"siap di {a.host}:{a.port} | strategi={a.strategy} | workers={a.workers}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        served, _ = state.snapshot()
        log("BROKER", f"dihentikan. Ringkasan request terlayani per worker: {served}")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
