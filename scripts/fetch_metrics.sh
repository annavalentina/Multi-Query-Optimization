#!/bin/env bash

rm -f storm_metrics/*

STORM_URL="NIMBUS_EXTERNAL_IP:8080"

topologies=$(http GET http://${STORM_URL}/api/v1/topology/summary | jq -r ".topologies[] | .id")

#create metrics files
for i in ${topologies[@]}; do
	touch ./storm_metrics/${i}.metrics
done


watch -n $1 ./metrics.sh $topologies