#!/bin/bash
# Run on the VM after every VM (re)start: starts the containers,
# the Hadoop cluster and HiveServer2.
sudo docker start hadoop airflow mysql mtg-frontend
sleep 20
sudo docker exec -u hadoop hadoop bash -lc "start-all.sh; nohup hiveserver2 > /home/hadoop/hiveserver2.log 2>&1 &"
echo "Services started. HiveServer2 needs about one more minute."
