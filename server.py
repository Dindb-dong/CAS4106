import socket, sys
from threading import Thread
import time
is_running = False

def setup_sockets():
  pass
def producer_worker():
  pass
  
def consumer_worker():
    pass

def shutdown():
  global is_running
  is_running = False

if __name__ == '__main__':
  setup_sockets()
  producer_thread = Thread(target = producer_worker)
  producer_thread.daemon = True
  producer_thread.start()
  worker_thread = Thread(target = consumer_worker)
  worker_thread.daemon = True
  worker_thread.start()

  try:
    while is_running:
      time.sleep(1)
  except KeyboardInterrupt:
    shutdown()
  finally:
    shutdown()