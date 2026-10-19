"""Uji ketahanan broker-worker: invalid request, worker mati, beban tinggi, recovery."""
import os
import socket
import subprocess
import sys
import threading
import time

PY = sys.executable
BROKER_PORT = 7000
WORKER_PORTS = [7101, 7102, 7103]
os.makedirs("logs_resilience", exist_ok=True)


def wait_port(port, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def spawn_worker(wid, port, tag):
    f = open(f"logs_resilience/{tag}_worker{wid}.log", "w")
    return subprocess.Popen(
        [PY, "-u", "worker.py", "--id", str(wid), "--port", str(port),
         "--long-delay", "0.3", "--short-delay", "0.02"],
        stdout=f, stderr=subprocess.STDOUT,
    )


def spawn_broker(tag, strategy="balanced"):
    f = open(f"logs_resilience/{tag}_broker.log", "w")
    workers = ",".join(f"127.0.0.1:{p}" for p in WORKER_PORTS)
    return subprocess.Popen(
        [PY, "-u", "broker.py", "--port", str(BROKER_PORT),
         "--strategy", strategy, "--workers", workers],
        stdout=f, stderr=subprocess.STDOUT,
    )


def send_raw(line, timeout=30):
    with socket.create_connection(("127.0.0.1", BROKER_PORT), timeout=timeout) as s:
        s.sendall((line + "\n").encode("utf-8"))
        return s.makefile("r", encoding="utf-8").readline().strip()


def get_stats():
    return send_raw("STATS", timeout=5)


def kill_all(procs):
    for p in procs:
        try:
            p.terminate()
        except Exception:
            pass
    for p in procs:
        try:
            p.wait(timeout=5)
        except Exception:
            pass


def section(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70, flush=True)


# ----------------- SKENARIO 1: Invalid requests -----------------
def test_invalid():
    section("TEST 1: Request tidak valid (app id salah / format rusak)")
    workers = [spawn_worker(i + 1, p, "t1") for i, p in enumerate(WORKER_PORTS)]
    broker = spawn_broker("t1")
    for p in WORKER_PORTS + [BROKER_PORT]:
        assert wait_port(p), f"port {p} tidak siap"

    cases = [
        "REQ 9 payload",         # app id tidak dikenal
        "HELLO 1 payload",       # bukan REQ
        "REQ",                   # kurang argumen
        "REQ abc payload",       # app id bukan angka
        "REQ 1 valid_payload",   # valid (kontrol)
        "REQ 2 echo_this",       # valid (kontrol)
    ]
    for c in cases:
        try:
            r = send_raw(c)
        except Exception as e:
            r = f"<exception: {e}>"
        print(f"  kirim {c!r:35s} -> {r}")

    print(f"  STATS akhir: {get_stats()}")
    kill_all(workers + [broker])


# ----------------- SKENARIO 2: 1 worker mati -----------------
def test_one_worker_down():
    section("TEST 2: Satu worker mati di tengah operasi")
    workers = [spawn_worker(i + 1, p, "t2") for i, p in enumerate(WORKER_PORTS)]
    broker = spawn_broker("t2", strategy="sequential")
    for p in WORKER_PORTS + [BROKER_PORT]:
        assert wait_port(p), f"port {p} tidak siap"

    print("  >>> Matikan WORKER-2 ...")
    workers[1].terminate()
    workers[1].wait(timeout=5)
    time.sleep(0.5)

    for i in range(6):
        try:
            r = send_raw(f"REQ 2 msg-{i}")
        except Exception as e:
            r = f"<exception: {e}>"
        print(f"  req #{i + 1}: {r}")

    print(f"  STATS akhir: {get_stats()}")
    kill_all([workers[0], workers[2], broker])


# ----------------- SKENARIO 3: Semua worker mati -----------------
def test_all_down():
    section("TEST 3: Semua worker mati, broker tetap melayani")
    broker = spawn_broker("t3")
    assert wait_port(BROKER_PORT), "broker tidak siap"

    for i in range(3):
        try:
            r = send_raw(f"REQ 1 msg-{i}", timeout=10)
        except Exception as e:
            r = f"<exception: {e}>"
        print(f"  req #{i + 1}: {r}")
    print(f"  STATS akhir: {get_stats()}")
    kill_all([broker])


# ----------------- SKENARIO 4: Beban concurrent tinggi -----------------
def test_high_load():
    section("TEST 4: Beban concurrent 50 request (SHORT) paralel")
    workers = [spawn_worker(i + 1, p, "t4") for i, p in enumerate(WORKER_PORTS)]
    broker = spawn_broker("t4", strategy="balanced")
    for p in WORKER_PORTS + [BROKER_PORT]:
        assert wait_port(p), f"port {p} tidak siap"

    N = 50
    results = [None] * N
    ok = [0]
    err = [0]
    lock = threading.Lock()

    def task(i):
        try:
            r = send_raw(f"REQ 2 c-{i}", timeout=30)
        except Exception as e:
            r = f"<exception: {e}>"
        with lock:
            if r.startswith("RES OK"):
                ok[0] += 1
            else:
                err[0] += 1
        results[i] = r

    t0 = time.time()
    threads = [threading.Thread(target=task, args=(i,)) for i in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - t0
    print(f"  total={N}, OK={ok[0]}, ERR={err[0]}, waktu={elapsed:.2f}s")
    print(f"  STATS akhir: {get_stats()}")

    distribusi = {}
    for r in results:
        if r.startswith("RES OK"):
            parts = r.split()
            wid = parts[2]
            distribusi[wid] = distribusi.get(wid, 0) + 1
    print(f"  distribusi per worker: {distribusi}")
    kill_all(workers + [broker])


# ----------------- SKENARIO 5: Recovery worker -----------------
def test_recovery():
    section("TEST 5: Worker mati lalu dihidupkan kembali (recovery)")
    workers = [spawn_worker(i + 1, p, "t5a") for i, p in enumerate(WORKER_PORTS)]
    broker = spawn_broker("t5", strategy="balanced")
    for p in WORKER_PORTS + [BROKER_PORT]:
        assert wait_port(p), f"port {p} tidak siap"

    print("  fase 1 (semua worker hidup, kirim 3 request):")
    for i in range(3):
        print(f"    {send_raw(f'REQ 2 p1-{i}')}")

    print("  >>> matikan WORKER-3 ...")
    workers[2].terminate()
    workers[2].wait(timeout=5)
    time.sleep(0.5)

    print("  fase 2 (W3 mati, kirim 3 request):")
    for i in range(3):
        try:
            print(f"    {send_raw(f'REQ 2 p2-{i}')}")
        except Exception as e:
            print(f"    <exception: {e}>")

    print("  >>> hidupkan kembali WORKER-3 ...")
    workers[2] = spawn_worker(3, WORKER_PORTS[2], "t5b")
    assert wait_port(WORKER_PORTS[2]), "W3 tidak siap"
    time.sleep(0.3)

    print("  fase 3 (W3 kembali, kirim 6 request):")
    for i in range(6):
        try:
            print(f"    {send_raw(f'REQ 2 p3-{i}')}")
        except Exception as e:
            print(f"    <exception: {e}>")
    print(f"  STATS akhir: {get_stats()}")
    kill_all(workers + [broker])


if __name__ == "__main__":
    test_invalid()
    time.sleep(0.5)
    test_one_worker_down()
    time.sleep(0.5)
    test_all_down()
    time.sleep(0.5)
    test_high_load()
    time.sleep(0.5)
    test_recovery()
    print("\n>>> SELESAI")
