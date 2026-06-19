#!/usr/bin/env python3
import sys
import time

import redis
import os


def start_server(server_key: str):
    print(f"[INFO] Starting server with key: {server_key}")

    # Example logic  replace with your actual server startup code
    
# SPIDER IMPLEMENTATION

    redishost= os.getenv("REDIS_HOST","localhost") #Permits setting a different redishost using env var - default is "localhost"

    red = redis.Redis(redishost, port=6379, db=0, decode_responses=True)
    keys = red.keys('ACTION_SERVER_IP:*')

    redis_key = f"ACTION_SERVER_IP:{server_key}"
    server_data = red.hgetall(redis_key)

    if not server_data:
        print(f"[FAILED] Server {server_key} not started! - No data found on Redis!")
        exit(0)
    else:
        for server_id, ip in server_data.items():
            print(f"Ident: {server_key}, ServerID: {server_id}, IP: {ip}")

            cmd="ssh "+ redishost + " 'manageServerREDIS /usr/local/mdsplus/local/jDispatcher_spider.properties " + server_key + " " + server_id + " start'"
            # print(f"RUNNING - {cmd}")
            os.system(cmd)
    
# END SPIDER IMPLEMENTATION


    time.sleep(1)
    print("[OK] Server started successfully!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python server_start.py {server_key}")
        sys.exit(1)

    server_key = sys.argv[1]
    start_server(server_key)



