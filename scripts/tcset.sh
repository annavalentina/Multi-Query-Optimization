#!/usr/bin/env bash

source "$(dirname $0)/incl.sh"

for worker in "${workers[@]}"; do
	i=0
	for neighbor in "${workers[@]}"; do
		if [ "$worker" = "$neighbor" ]; then
			continue
		fi
		latency=$(jq .${worker}.latency.${neighbor} $1)
		bandwidth=$(jq .${worker}.bandwidth.${neighbor} $1)
		if [ "null" != "$latency" ] && [ "null" != "$bandwidth" ]; then
			echo "Setting rules for ${worker} - ${neighbor} pair..."
			if [ $i -eq 0 ]; then
				add=""
			else
				add="--add"
			fi
			ssh ${worker} "echo ${password} | sudo -S tcset ${adapters[$worker]} --rate $(echo "0.1 * ${bandwidth}" | bc )Mbps --delay $(echo "50 * ${latency}" | bc )ms --network ${ips[$neighbor]} ${add}> /dev/null 2>&1"
			i=$((i + 1))
		fi
	done
done