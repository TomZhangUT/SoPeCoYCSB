#!/bin/bash
NUM_HOSTS=$1
HOSTS=$2

if [ "$NUM_HOSTS" = "1" ]; then

THREADS=128
TOKENS=0

elif [ "$NUM_HOSTS" = "2" ]; then

THREADS=256
TOKENS=(0 85070591730234615865843651857942052864)
fi

HOST_ARRAY=(`echo $HOSTS | sed -e 's/,/\n/g'`)

CASSANDRA_ROOTFOLDER=~/work/n1/apache-cassandra-1.2.8

echo "Resetting DB"
$CASSANDRA_ROOTFOLDER/bin/cassandra-cli --host n1 --port 9160 -f ~/work/n1/reset-db.txt

for((i=0;i < $NUM_HOSTS;i++))
do
    echo ${HOST_ARRAY[$i]}
    $CASSANDRA_ROOTFOLDER/bin/nodetool -h ${HOST_ARRAY[$i]} disablegossip
    $CASSANDRA_ROOTFOLDER/bin/nodetool -h ${HOST_ARRAY[$i]} disablethrift
    $CASSANDRA_ROOTFOLDER/bin/nodetool -h ${HOST_ARRAY[$i]} drain
done

pdsh -R ssh -w $HOSTS pkill -f apache-cassandra

sleep 30

pdsh -R ssh -w $HOSTS killall -9 -u $USER

echo "shutoff cassandra"
rm -rf ~/work/n1/data/lib/cassandra/*

