#!/usr/bin/env bash


# delete all pre-existing rules
for worker in "${workers[@]}"; do
	echo "Deleting rules for worker ${worker}..."
	ssh ${worker} "echo ${password} | sudo -S tcdel ${adapters[$worker]} --all > /dev/null 2>&1"
done