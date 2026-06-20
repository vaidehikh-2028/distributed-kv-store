"""
Tiny CLI client for the distributed key-value store.

Usage:
    python client.py <host> <port>

Example:
    python client.py 127.0.0.1 9000   # talk to the leader
    python client.py 127.0.0.1 9100   # talk to a follower (read-only)
"""

import socket
import sys


def main():
    if len(sys.argv) != 3:
        print("Usage: python client.py <host> <port>")
        return

    host, port = sys.argv[1], int(sys.argv[2])
    sock = socket.create_connection((host, port))
    print(f"Connected to {host}:{port}. Try: SET name alice   or   GET name")

    buffer = ""
    try:
        while True:
            line = input("> ").strip()
            if not line:
                continue
            sock.sendall((line + "\n").encode())
            while "\n" not in buffer:
                data = sock.recv(1024)
                if not data:
                    print("Connection closed by server")
                    return
                buffer += data.decode()
            response, buffer = buffer.split("\n", 1)
            print(response)
    except (KeyboardInterrupt, EOFError):
        print("\nClosing connection.")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
