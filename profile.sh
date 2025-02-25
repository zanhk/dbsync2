#!/bin/bash
HOST=localhost
PROFILE_DIR=profiles
mkdir -p $PROFILE_DIR

for i in `seq 1 10`;
do
   echo "Create heap profile $i.heap"
   curl -s http://$HOST:8080/debug/pprof/heap > $PROFILE_DIR/$i.heap
   sleep 5;
done

go tool pprof -base $PROFILE_DIR/1.heap $PROFILE_DIR/10.heap
