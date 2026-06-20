"""
Follower node of a minimal distributed key-value store.

Responsibilities:
  - Connect to the leader and continuously apply the stream of writes it sends.
  - Serve GET requests from clients out of its own (replicated) copy of the data.
  - Reject SET requests from clients -- all writes must go through the leader.

This means a follower's data can briefly lag behind the leader's (the writes
are applied asynchronously). That lag is "replication lag," a real concept
in production distributed systems -- worth knowing the term for an interview.

Run with an optional port argument so you can run several followers at once:
    python follower.py 9100
    python follower.py 9101
"""

import socket
import sys
import threading
import time

HOST = "0.0.0.0"
CLIENT_PORT = 9100
LEADER_HOST = "127.0.0.1"
LEADER_REPLICA_PORT = 9001

store = {}
store_lock = threading.Lock()


def apply_replicated_command(line: str) -> None:
    parts = line.split(" ", 2)
    if parts[0].upper() == "SET" and len(parts) == 3:
        key, value = parts[1], parts[2]
        with store_lock:
            store[key] = value
        print(f"[follower] applied replicated SET {key}={value}")


def replication_listener() -> None:
    """Stay connected to the leader and apply every write it streams down."""
    while True:
        try:
            sock = socket.create_connection((LEADER_HOST, LEADER_REPLICA_PORT))
            print("[follower] connected to leader for replication")
            buffer = ""
            while True:
                data = sock.recv(1024)
                if not data:
                    raise ConnectionError("leader closed the replication socket")
                buffer += data.decode()
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if line.strip():
                        apply_replicated_command(line.strip())
        except OSError as e:
            print(f"[follower] lost connection to leader ({e}); retrying in 2s")
            time.sleep(2)


def process_command(line: str) -> str:
    parts = line.split(" ", 2)
    cmd = parts[0].upper() if parts else ""

    if cmd == "GET" and len(parts) == 2:
        with store_lock:
            value = store.get(parts[1])
        return f"VALUE {value}" if value is not None else "NOTFOUND"

    if cmd == "SET":
        return "ERROR writes must go to the leader"

    return "ERROR unknown command"


def handle_client(conn: socket.socket, addr) -> None:
    buffer = ""
    with conn:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            buffer += data.decode()
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if line:
                    conn.sendall((process_command(line) + "\n").encode())


def accept_loop(port: int) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, port))
    server.listen()
    print(f"[follower] listening for clients on port {port}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        CLIENT_PORT = int(sys.argv[1])
    threading.Thread(target=replication_listener, daemon=True).start()
    accept_loop(CLIENT_PORT)
