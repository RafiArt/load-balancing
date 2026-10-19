"""Client: mengirim request ke broker dengan menyebutkan Application ID."""
import argparse
import socket
import threading
import time

from common import APPS, DEFAULT_HOST, parse_app


def send_request(host, port, app_id, payload, timeout=120):
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.sendall(f"REQ {app_id} {payload}\n".encode("utf-8"))
        return s.makefile("r", encoding="utf-8").readline().strip()


def get_stats(host, port):
    with socket.create_connection((host, port), timeout=10) as s:
        s.sendall(b"STATS\n")
        return s.makefile("r", encoding="utf-8").readline().strip()


def main():
    ap = argparse.ArgumentParser(description="Client broker-worker")
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--apps", default="short",
                    help="pola application id yang diulang, mis. 'long,short,short' atau '1,2,2'")
    ap.add_argument("--payload", default="hello", help="isi request (LONG: batas N bilangan prima)")
    ap.add_argument("--count", type=int, default=1, help="jumlah request")
    ap.add_argument("--delay", type=float, default=0.1, help="jeda antar pengiriman request (detik)")
    ap.add_argument("--stats", action="store_true", help="tampilkan STATS broker di akhir")
    a = ap.parse_args()

    pattern = [parse_app(t) for t in a.apps.split(",")]
    if None in pattern:
        raise SystemExit("application id harus 1/2 atau long/short")

    results = [None] * a.count

    def worker(i, app_id):
        t0 = time.time()
        try:
            reply = send_request(a.host, a.port, app_id, a.payload)
        except OSError as e:
            reply = f"RES ERR {e}"
        results[i] = (app_id, reply, time.time() - t0)

    t_start = time.time()
    threads = []
    for i in range(a.count):
        app_id = pattern[i % len(pattern)]
        print(f"[CLIENT] kirim request #{i + 1}: REQ {app_id} {a.payload}  ({APPS[app_id]})", flush=True)
        t = threading.Thread(target=worker, args=(i, app_id))
        t.start()
        threads.append(t)
        time.sleep(a.delay)
    for t in threads:
        t.join()
    wall = time.time() - t_start

    print("\n=== HASIL ===")
    per_worker = {}
    for i, (app_id, reply, dt) in enumerate(results, 1):
        print(f"#{i:<2} app={APPS[app_id]:<5} latency={dt:5.2f}s  {reply}")
        parts = reply.split(" ", 4)
        if parts[:2] == ["RES", "OK"]:
            per_worker.setdefault(int(parts[2]), []).append(dt)
    print("\n=== RINGKASAN ===")
    for w in sorted(per_worker):
        lat = per_worker[w]
        print(f"worker-{w}: {len(lat)} request, rata-rata latency {sum(lat) / len(lat):.2f}s")
    all_lat = [x for v in per_worker.values() for x in v]
    if all_lat:
        print(f"total request OK: {len(all_lat)}/{a.count} | rata-rata latency: "
              f"{sum(all_lat) / len(all_lat):.2f}s | waktu total: {wall:.2f}s")
    if a.stats:
        print(get_stats(a.host, a.port))


if __name__ == "__main__":
    main()
