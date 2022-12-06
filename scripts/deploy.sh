#!/bin/env bash


echo "Deleting rules..."
./tcdel.sh
echo "Removing any previous storm deployment..."
ssh anaconda "docker stack rm storm" || continue
sleep 10
echo "Deploying storm..."
ssh anaconda "docker stack deploy -c docker-compose.yml storm"
sleep 10
echo "Setting throttles..."
./tcset.sh ./network.json

echo "Give some time for storm to initialize..."
sleep 20

echo "Running topologies...."

ssh anaconda "./run_topology.sh topologyCreator ReplicatedTopology replicated 1 0 50.0,20.0,20.0,30.0,30.0 10000 2000"
ssh anaconda "./run_topology.sh topologyCreator DiamondTopology diamond 6 5 50.0,20.0,20.0,20.0,30.0,30.0 10000 1000"
ssh anaconda "./run_topology.sh topologyCreator LinearTopology linear 6 11 50.0,20.0,20.0,20.0,30.0,30.0 10000 4000"
# sleep for a minute for data to be generated (optional)
# sleep 60

echo "Fetching metrics every 10 seconds..." # for 15 minutes..."
./fetch_metrics.sh 10 
echo "Finished!"
