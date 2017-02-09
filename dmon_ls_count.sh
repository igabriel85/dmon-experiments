#!/usr/bin/env bash
#Enter the number of threads created
thread=$1
rt=$((thread-1))
for i in $(seq 0 $rt);
do
   echo "Number of messages received from Thread $i"
   cat results | grep -i "Thread id $i" | tee thread-$i.out | wc -l
done