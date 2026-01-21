import os
import multiprocessing
from redis import Redis
from rq import Worker, Queue, Connection
from app import app # Necessary to load DB context

# Get Redis URL
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = Redis.from_url(redis_url)

def start_worker(queue_name):
    """
    Starts a dedicated worker process for a specific queue.
    """
    print(f"👷 Starting Worker for '{queue_name}' queue...")
    
    with Connection(conn):
        # We listen ONLY to the specific queue assigned to this process
        w = Worker([Queue(queue_name)], name=f"Worker-{queue_name}")
        
        # CRITICAL: Push app context so database queries work inside jobs
        with app.app_context():
            w.work()

if __name__ == '__main__':
    print("🚀 Launching Parallel Workers...")

    # Process 1: The Fast Lane (Inventory, Orders, Fulfillments)
    # This worker sits idle until a critical job arrives.
    p1 = multiprocessing.Process(target=start_worker, args=('critical',))
    
    # Process 2: The Heavy Lane (Products, Images, Maintenance)
    # This worker handles the long-running stuff.
    p2 = multiprocessing.Process(target=start_worker, args=('default',))

    # Start both processes simultaneously
    p1.start()
    p2.start()
    
    # Keep the main script alive while workers run
    p1.join()
    p2.join()
