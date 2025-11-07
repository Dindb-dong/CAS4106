import socket, time, sys

host = sys.argv[1]
port = int(sys.argv[2])
task_file = sys.argv[3]
sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sc.connect((host, port))
tasks = []
with open(task_file, 'r') as f:
  for line in f:
    timestamp, priority, task_id, duration = line.strip().split()
    tasks.append((timestamp, priority, task_id, duration))
for timestamp, priority, task_id, duration in tasks:
  sc.send(f'CREATE {priority} {task_id} {duration}\n'.encode())
sc.close()