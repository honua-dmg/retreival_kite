import redis
import os
import dotenv

ENVLOC = '/app/.env'
dotenv.load_dotenv(ENVLOC)
# Centralized Redis connection
# Use a single connection pool that is shared across the application.
r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True
)
STOCKS = os.getenv("STOCKS").split(",")