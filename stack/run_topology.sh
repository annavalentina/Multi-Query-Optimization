#!/bin/env bash
docker exec $(docker ps -q -f name=nimbus) storm jar /jars/$1.jar $2 $3 $4 $5 $6 $7 $8
