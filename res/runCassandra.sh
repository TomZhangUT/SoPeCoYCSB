#!/bin/bash
NUM_HOSTS=$1

TWO_HOSTS=10.0.1.1,10.0.1.2
ONE_HOSTS=127.0.0.1

if [ "$NUM_HOSTS" = "1" ]; then

CLIENTS=10.0.1.3
HOSTS=${ONE_HOST}
THREADS=128
TOKENS=0

elif [ "$NUM_HOSTS" = "2" ]; then

CLIENTS=10.0.1.3
HOSTS=${TWO_HOSTS}
THREADS=256
TOKENS=(0 85070591730234615865843651857942052864)
fi

HOST_ARRAY=(`echo $HOSTS | sed -e 's/,/\n/g'`)

CASSANDRA_ROOTFOLDER=~/Documents/apache-cassandra-1.2.6
sed -i "s/- seeds:.*/- seeds: \"$HOSTS\"/g"      $CASSANDRA_ROOTFOLDER/conf/cassandra.yaml
sed -i "s|/var\/lib\/cassandra/|$CASSANDRA_ROOTFOLDER/|g"      $CASSANDRA_ROOTFOLDER/conf/cassandra.yaml
bash ${CASSANDRA_ROOTFOLDER}/bin/cassandra
sleep 20

for((i=0;i<$NUM_HOSTS;i++))
do
	$CASSANDRA_ROOTFOLDER/bin/nodetool -h ${HOST_ARRAY[$i]} move ${TOKENS[$i]} 
done

echo "Start experiment"
