#!/bin/bash
cd /root/retreival_kite
docker-compose down 
DATA_HOST_PATH=/root/data docker compose up app