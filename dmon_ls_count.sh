#!/usr/bin/env bash
thread=$1
rt=$((thread-1))
for i in $(seq 1 $rt);
do
   echo "Number of messages received from Thread $i"
   cat results | grep -i "Thread id $i" | tee thread-$i.out | wc -l
done