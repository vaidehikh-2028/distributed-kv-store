import socket
import time


def send(host, port, command):
    sock = socket.create_connection((host, port), timeout=3)
    sock.sendall((command + "\n").encode())
    response = sock.recv(1024).decode().strip()
    sock.close()
    return response


print("1. SET on leader:", send("127.0.0.1", 9000, "SET name alice"))
time.sleep(0.5)  # give replication a moment to propagate

print("2. GET on leader:", send("127.0.0.1", 9000, "GET name"))
print("3. GET on follower A (9100):", send("127.0.0.1", 9100, "GET name"))
print("4. GET on follower B (9101):", send("127.0.0.1", 9101, "GET name"))

print("5. SET attempt on follower (should be rejected):",
      send("127.0.0.1", 9100, "SET hacked yes"))

print("6. GET missing key on leader:", send("127.0.0.1", 9000, "GET missing"))
