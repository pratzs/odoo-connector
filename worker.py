import os
import multiprocessing
import uuid
from redis import Redis
from rq import Worker, Queue, Connection
from app import app

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = Redis.from_url(redis_url)

def start_worker(queue_name):
    """
    Starts a dedicated worker process for a specific queue.
    """
    # FIX: Use UUID to ensure unique name even if PID is reused during restart
    # This prevents "ValueError: There exists an active worker..."
    unique_id = str(uuid.uuid4())[:8]
    unique_name = f"Worker-{queue_name}-{unique_id}"

    print(f"👷 Starting {unique_name} for '{queue_name}' queue...")

    # FIX: Pass connection directly to Queue and Worker instead of using
    # the deprecated Connection context manager (removed in RQ 2.x).
    q = Queue(queue_name, connection=conn)
    w = Worker([q], connection=conn, name=unique_name)

    with app.app_context():
        w.work()

if __name__ == '__main__':
    print("🚀 Launching Parallel Workers...")

    # Process 1: Critical Lane (Orders, Inventory)
    p1 = multiprocessing.Process(target=start_worker, args=('critical',))
    
    # Process 2: Default Lane (Products, Images)
    p2 = multiprocessing.Process(target=start_worker, args=('default',))

    p1.start()
    p2.start()
    
    p1.join()
    p2.join()
