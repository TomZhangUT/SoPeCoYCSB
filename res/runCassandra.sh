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

CASSANDRA_ROOTFOLDER=~/work/n1/apache-cassandra-1.2.8

HOST_ARRAY=(`echo $HOSTS | sed -e 's/,/\n/g'`)

#export CASSANDRA_INCLUDE=${CASSANDRA_ROOTFOLDER}/bin/cassandra.in.sh
#export CASSANDRA_HOME=$CASSANDRA_ROOTFOLDER


sed -i "s/- seeds:.*/- seeds: \"$HOSTS\"/g"      $CASSANDRA_ROOTFOLDER/conf/cassandra.yaml
sed -i "s|/var/lib/cassandra/|~/work/n1/data/lib/cassandra/|g"      $CASSANDRA_ROOTFOLDER/conf/cassandra.yaml
sed -i "s|-Xss180k|-Xss256k|g"      $CASSANDRA_ROOTFOLDER/conf/cassandra-env.sh
sed -i "s|/var/log/cassandra/|~/work/n1/data/lib/cassandra/|g"      $CASSANDRA_ROOTFOLDER/conf/log4j-server.properties
pdsh -R ssh -w $HOSTS bash ${CASSANDRA_ROOTFOLDER}/bin/cassandra
sleep 30

echo "Changing tokens"
#for((i=0;i < $NUM_HOSTS;i++))
#do
#    $CASSANDRA_ROOTFOLDER/bin/nodetool -h ${HOST_ARRAY[$i]} move ${TOKENS[$i]} 
#done

echo "Creating DB"
$CASSANDRA_ROOTFOLDER/bin/cassandra-cli --host n1 --port 9160 -f ~/work/n1/create-db.txt

echo "Start experiment"


