import socket, time, sys

# 명령행 인자 파싱
host = sys.argv[1]
port = int(sys.argv[2])

try:
    # 서버에 연결
    sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sc.connect((host, port))
    
    # 서버로부터 Consumer ID 수신
    consumer_id = sc.recv(1024).decode().strip()
    
    # 무한 루프로 작업 요청
    while True:
        # 서버에 작업 요청
        sc.send(b'REQUEST\n')
        print('[REQUEST]')
        
        # 서버로부터 응답 수신
        response = sc.recv(1024).decode().strip()
        
        if response == 'DONE':
            # 서버가 종료 신호를 보냄
            break
        
        elif response.startswith('ASSIGN'):
            # 작업 할당 받음
            parts = response.split()
            if len(parts) == 3:
                _, task_id, duration = parts
                duration = float(duration)
                
                # 작업 처리 시뮬레이션 (duration만큼 대기)
                time.sleep(duration)
                
                # 작업 완료 알림
                sc.send(f'COMPLETE {task_id}\n'.encode())
                print(f'[COMPLETE] {task_id}')
        
        elif response == 'NOTASK':
            # 할당할 작업이 없음
            print('NOTASK')
            # 1초 대기 후 다시 요청
            time.sleep(1)
        else:
            # 기타 응답 처리
            print(response)
            time.sleep(1)
    
    # 연결 종료
    sc.close()
except KeyboardInterrupt:
    print('exit')
    if 'sc' in locals():
        sc.close()
    sys.exit(0)
