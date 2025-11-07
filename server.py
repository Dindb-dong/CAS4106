import socket, sys
from threading import Thread, Lock
import time
import heapq

# 전역 변수
is_running = False
producer_socket = None
consumer_socket = None
priority_queue = []  # 우선순위 큐 (heapq 사용)
queue_lock = Lock()  # 큐 접근 동기화를 위한 락
consumer_counter = 0  # Consumer ID 카운터
consumer_counter_lock = Lock()  # Consumer 카운터 동기화를 위한 락

def setup_sockets():
    """서버 소켓 초기화: Producer와 Consumer를 위한 별도 포트 설정"""
    global producer_socket, consumer_socket, is_running
    
    if len(sys.argv) != 4:
        print("Usage: python server.py <IP> <Producer포트> <Consumer포트>")
        sys.exit(1)
    
    ip = sys.argv[1]
    producer_port = int(sys.argv[2])
    consumer_port = int(sys.argv[3])
    
    # Producer 소켓 설정
    producer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    producer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    producer_socket.bind((ip, producer_port))
    producer_socket.listen(5)
    
    # Consumer 소켓 설정
    consumer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    consumer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    consumer_socket.bind((ip, consumer_port))
    consumer_socket.listen(5)
    
    is_running = True
    print(f"[Server] Producer 포트: {producer_port}, Consumer 포트: {consumer_port}에서 대기 중...")

def handle_producer(client_socket, addr):
    """Producer 연결을 처리하는 함수"""
    try:
        while is_running:
            # Producer로부터 메시지 수신
            data = client_socket.recv(1024).decode()
            if not data:
                break
            
            # CREATE 명령 처리
            if data.startswith('CREATE'):
                parts = data.strip().split()
                if len(parts) == 4:
                    _, priority, task_id, duration = parts
                    priority = int(priority)
                    
                    # 우선순위 큐에 작업 추가 (우선순위가 낮을수록 높은 우선순위)
                    with queue_lock:
                        heapq.heappush(priority_queue, (priority, task_id, duration))
                    
                    print(f"[CREATE] {priority} {task_id} {duration}")
    except Exception as e:
        print(f"[Error] Producer 처리 중 오류: {e}")
    finally:
        client_socket.close()

def handle_consumer(client_socket, addr):
    """Consumer 연결을 처리하는 함수"""
    global consumer_counter
    
    # Consumer에게 고유 ID 할당
    with consumer_counter_lock:
        consumer_counter += 1
        consumer_id = f"Consumer{consumer_counter}"
    
    try:
        # Consumer에게 ID 전송
        client_socket.send(consumer_id.encode())
        print(f"[{consumer_id} connected]")
        print(f"{consumer_counter} consumers online")
        while is_running:
            # Consumer로부터 REQUEST 메시지 수신
            data = client_socket.recv(1024).decode()
            if not data:
                break
            
            if data.strip() == 'REQUEST':
                with queue_lock:
                    if priority_queue:
                        # 우선순위가 가장 높은 작업 꺼내기
                        priority, task_id, duration = heapq.heappop(priority_queue)
                        # Consumer에게 작업 할당
                        message = f"ASSIGN {task_id} {duration}\n"
                        client_socket.send(message.encode())
                        print(f"[ASSIGN] {task_id} - {consumer_id}")
                    else:
                        # 큐가 비어있으면 NOTASK 응답
                        client_socket.send(b'NOTASK\n')
            
            elif data.strip().startswith('COMPLETE'):
                # Consumer가 작업 완료를 알림
                parts = data.strip().split()
                if len(parts) == 2:
                    task_id = parts[1]
                    print(f"[COMPLETE] {task_id} from {consumer_id}")
    except Exception as e:
        print(f"[Error] Consumer 처리 중 오류: {e}")
    finally:
        client_socket.close()
        print(f"[{consumer_id} disconnected]")
        print(f"{consumer_counter} consumers online")

def producer_worker():
    """Producer 연결을 받는 워커 스레드"""
    while is_running:
        try:
            client_socket, addr = producer_socket.accept()
            # 각 Producer 연결을 별도 스레드에서 처리
            thread = Thread(target=handle_producer, args=(client_socket, addr))
            thread.daemon = True
            thread.start()
        except Exception as e:
            if is_running:
                print(f"[Error] Producer 연결 수락 중 오류: {e}")

def consumer_worker():
    """Consumer 연결을 받는 워커 스레드"""
    while is_running:
        try:
            client_socket, addr = consumer_socket.accept()
            # 각 Consumer 연결을 별도 스레드에서 처리
            thread = Thread(target=handle_consumer, args=(client_socket, addr))
            thread.daemon = True
            thread.start()
        except Exception as e:
            if is_running:
                print(f"[Error] Consumer 연결 수락 중 오류: {e}")

def shutdown():
    """서버 종료 처리"""
    global is_running, producer_socket, consumer_socket
    is_running = False
    if producer_socket:
        producer_socket.close()
    if consumer_socket:
        consumer_socket.close()
    print("exit")

if __name__ == '__main__':
    setup_sockets()
    producer_thread = Thread(target=producer_worker)
    producer_thread.daemon = True
    producer_thread.start()
    worker_thread = Thread(target=consumer_worker)
    worker_thread.daemon = True
    worker_thread.start()

    try:
        while is_running:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown()
    finally:
        shutdown()
