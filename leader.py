"""
Leader node of a minimal distributed key-value store.

Responsibilities:
  - Accept client connections and serve SET / GET commands.
  - Be the single source of truth for the data.
  - Accept connections from follower nodes and stream every write to them.

Concurrency model:
  - One thread per client connection, one thread per follower connection.
  - `store_lock` protects the in-memory dict from concurrent reads/writes.
  - `followers_lock` protects the list of connected follower sockets.

This is intentionally a "fire-and-forget" (asynchronous) replication model:
the leader commits the write locally and replies OK to the client WITHOUT
waiting for followers to acknowledge it. That's a real design choice with
a real trade-off -- see README.md for the synchronous alternative and why
you might pick one over the other.
"""

import socket
import threading

HOST = "0.0.0.0"
CLIENT_PORT = 9000
REPLICA_PORT = 9001

store = {}
store_lock = threading.Lock()

followers = []
followers_lock = threading.Lock()


def replicate(command_line: str) -> None:
    """Forward a write command to every currently connected follower."""
    with followers_lock:
        dead = []
        for sock in followers:
            try:
                sock.sendall((command_line + "\n").encode())
            except OSError:
                dead.append(sock)
        for sock in dead:
            followers.remove(sock)


def process_command(line: str) -> str:
    parts = line.split(" ", 2)
    cmd = parts[0].upper() if parts else ""

    if cmd == "SET" and len(parts) == 3:
        key, value = parts[1], parts[2]
        with store_lock:
            store[key] = value
        # Commit locally first, THEN replicate. The leader's own copy is
        # always authoritative even if replication to a follower fails.
        replicate(line)
        return "OK"

    if cmd == "GET" and len(parts) == 2:
        with store_lock:
            value = store.get(parts[1])
        return f"VALUE {value}" if value is not None else "NOTFOUND"

    return "ERROR unknown command"


def handle_client(conn: socket.socket, addr) -> None:
    print(f"[leader] client connected: {addr}")
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
    print(f"[leader] client disconnected: {addr}")


def handle_follower(conn: socket.socket, addr) -> None:
    print(f"[leader] follower connected: {addr}")
    with followers_lock:
        followers.append(conn)
    try:
        # We don't expect data FROM the follower on this socket, but a
        # blocking recv() is the simplest way to detect when it disconnects.
        while conn.recv(1024):
            pass
    except OSError:
        pass
    finally:
        with followers_lock:
            if conn in followers:
                followers.remove(conn)
        print(f"[leader] follower disconnected: {addr}")


def accept_loop(port: int, handler) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, port))
    server.listen()
    print(f"[leader] listening on port {port}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handler, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    threading.Thread(
        target=accept_loop, args=(REPLICA_PORT, handle_follower), daemon=True
    ).start()
    accept_loop(CLIENT_PORT, handle_client)
