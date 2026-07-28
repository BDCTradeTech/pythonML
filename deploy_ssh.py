import os
import paramiko
import time

HOST = "157.230.88.160"
USER = "root"
KEY_PATH = os.path.expanduser("~/.ssh/id_ed25519")

commands = [
    ("git pull", "cd /opt/pythonml && git pull origin main"),
    ("restart service", "sudo systemctl restart pythonml"),
    ("sleep 3", "sleep 3"),
    ("service status", "sudo systemctl status pythonml"),
]

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

print(f"Connecting to {HOST} as {USER} (key: {KEY_PATH})...")
client.connect(HOST, username=USER, key_filename=KEY_PATH, timeout=30)
print("Connected.\n")

for label, cmd in commands:
    print(f"--- [{label}] ---")
    print(f"$ {cmd}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=60)
    out = stdout.read().decode()
    err = stderr.read().decode()
    if out:
        print(out.encode("utf-8", errors="replace").decode("utf-8"), end="")
    if err:
        print("[stderr]:", err.encode("utf-8", errors="replace").decode("utf-8"), end="")
    print()

client.close()
print("Connection closed.")
