"""Menjalankan seluruh simulasi otomatis: 3 worker + broker + client, untuk dua strategi."""
import os
import socket
import subprocess
import sys
import time

PY = sys.executable
BROKER_PORT = 5000
WORKER_PORTS = [6001, 6002, 6003]
os.makedirs("logs", exist_ok=True)


def port_in_use(port):
    try:
        socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
        return True
    except OSError:
        return False


def wait_port(port, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"port {port} tidak siap")


def run(name, strategy, apps, count):
    print(f"\n##### Skenario {name} | strategi={strategy} | pola={apps} | {count} request #####", flush=True)
    busy = [p for p in WORKER_PORTS + [BROKER_PORT] if port_in_use(p)]
    if busy:
        raise SystemExit(f"Port {busy} sudah dipakai proses lama. Matikan broker/worker yang masih jalan, lalu ulangi.")
    procs = []
    for i, p in enumerate(WORKER_PORTS, 1):
        f = open(f"logs/{name}_{strategy}_worker{i}.log", "w")
        procs.append(subprocess.Popen([PY, "-u", "worker.py", "--id", str(i), "--port", str(p)],
                                      stdout=f, stderr=subprocess.STDOUT))
    fb = open(f"logs/{name}_{strategy}_broker.log", "w")
    procs.append(subprocess.Popen([PY, "-u", "broker.py", "--port", str(BROKER_PORT), "--strategy", strategy],
                                  stdout=fb, stderr=subprocess.STDOUT))
    for p in WORKER_PORTS + [BROKER_PORT]:
        wait_port(p)
    out = subprocess.run([PY, "-u", "client.py", "--port", str(BROKER_PORT), "--apps", apps,
                          "--count", str(count), "--delay", "0.1", "--stats"],
                         capture_output=True, text=True).stdout
    with open(f"logs/{name}_{strategy}_client.log", "w") as f:
        f.write(out)
    print(out, flush=True)
    for p in procs:
        p.terminate()
    for p in procs:
        p.wait()
    time.sleep(0.5)


if __name__ == "__main__":
    for strategy in ("sequential", "balanced"):
        run("A_uniform", strategy, "short", 9)
    for strategy in ("sequential", "balanced"):
        run("B_campuran", strategy, "long,short,short", 9)
