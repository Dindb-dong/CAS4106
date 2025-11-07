# CAS4106, 2023122004 김동욱
# producer.py

import socket, time, sys

# 명령행 인자 파싱
host = sys.argv[1]
port = int(sys.argv[2])
task_file = sys.argv[3]

# 서버에 연결
sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sc.connect((host, port))

try:
  # 태스크 파일 읽기
  tasks = []
  with open(task_file, 'r') as f:
      for line in f:
          timestamp, priority, task_id, duration = line.strip().split()
          tasks.append((float(timestamp), priority, task_id, duration))

  # 프로그램 시작 시간 기록
  start_time = time.time()

  # 각 태스크를 timestamp에 맞춰 전송
  for timestamp, priority, task_id, duration in tasks:
      # timestamp만큼 대기 (프로그램 시작 후 경과 시간)
      elapsed = time.time() - start_time
      wait_time = timestamp - elapsed
      
      if wait_time > 0:
          time.sleep(wait_time)
      
      # 서버에 CREATE 명령 전송
      message = f'CREATE {priority} {task_id} {duration}\n'
      sc.send(message.encode())
      print(f'[CREATE] {priority} {task_id} {duration}')

  # 모든 작업 전송 완료 후 연결 종료
  print("exit")
  sc.close()
except KeyboardInterrupt:
  print("exit")
  sc.close()