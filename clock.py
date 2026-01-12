import logging
from app import run_schedule

# Configure logging to show up in your Render/Heroku logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Clock')

if __name__ == '__main__':
    logger.info("🕒 CLOCK PROCESS STARTED")
    logger.info("Waiting for scheduled tasks (Inventory: 10m, Orders: 3m)...")
    
    # This runs the infinite loop defined in app.py
    # It queries the DB for active shops and enqueues jobs to Redis.
    run_schedule()
