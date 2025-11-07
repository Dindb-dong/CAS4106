import socket, time, sys

host = sys.argv[1]
port = int(sys.argv[2])
worker_name = sys.argv[3]
sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sc.connect((host, port))
consumer_id = sc.recv(1024).decode().strip()
while True:
  sc.send(b'REQUEST\n')
  response = sc.recv(1024).decode().strip()
  if response == 'DONE':
    break
  print(response)
  time.sleep(1)
sc.close()