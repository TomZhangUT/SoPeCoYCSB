#!/bin/bash

YCSB_HOME=/home/tom/git/YCSB

i="0"

while [ $# -gt 0 ]
do
    if [ "$i" -eq "0" ]; then
  	DOTRANSACTION=$1
    elif [ "$i" -eq "1" ]; then
  	DBTYPE=$1
    elif [ "$i" -eq "2" ]; then
  	WORKLOAD=$1
    elif [ "$i" -eq "3" ]; then
  	HOSTS=$1
    elif [ "$i" -eq "4" ]; then
  	THREADS=$1
    elif [ "$i" -eq "5" ]; then
  	TARGET=$1
    elif [ "$i" -eq "6" ]; then
  	RECORDCOUNT=$1
    elif [ "$i" -eq "7" ]; then
  	OPERATIONCOUNT=$1
    elif [ "$i" -eq "8" ]; then
  	INSERTCOUNT=$1
    elif [ "$i" -eq "9" ]; then
  	FIELDCOUNT=$1
    elif [ "$i" -eq "10" ]; then
  	FIELDLENGTH=$1
    elif [ "$i" -eq "11" ]; then
  	MAXSCANLENGTH=$1
    elif [ "$i" -eq "12" ]; then
  	MAXEXECUTIONTIME=$1
    elif [ "$i" -eq "13" ]; then
  	READPROPORTION=$1
    elif [ "$i" -eq "14" ]; then
  	INSERTPROPORTION=$1
    elif [ "$i" -eq "15" ]; then
  	SCANPROPORTION=$1
    elif [ "$i" -eq "16" ]; then
  	INPUTPROPORTION=$1
    elif [ "$i" -eq "17" ]; then
  	UPDATEPROPORTION=$1
    else
	echo "something went wrong"
    fi
    i=$[$i+1]
    shift
done

if [ "$DOTRANSACTIONS" != "false" ]; then
echo "Running Workload"
echo $YCSB_HOME/bin/ycsb run $DBTYPE -p workload=$WORKLOAD \
-p hosts=$HOSTS \
-p threadcount=$THREADS \
-p target=$TARGET \
-p recordcount=$RECORDCOUNT \
-p operationcount=$OPERATIONCOUNT \
-p insertcount=$INSERTCOUNT \
-p maxexecutiontime=$MAXEXECUTIONTIME \
-p fieldcount=$FIELDCOUNT \
-p fieldlength=$FIELDLENGTH \
-p maxscanlength=$MAXSCANLENGTH \
-p readproportion=$READPROPORTION \
-p insertproportion=$INSERTPROPORTION \
-p scanproportion=$SCANPROPORTION \
-p inputproportion=$INPUTPROPORTION \
-p updateproportion=$UPDATEPROPORTION \
-p measurementtype=timeseries -s

python $YCSB_HOME/bin/ycsb run $DBTYPE -p workload=$WORKLOAD \
-p hosts=$HOSTS \
-p threadcount=$THREADS \
-p target=$TARGET \
-p recordcount=$RECORDCOUNT \
-p operationcount=$OPERATIONCOUNT \
-p insertcount=$INSERTCOUNT \
-p maxexecutiontime=$MAXEXECUTIONTIME \
-p fieldcount=$FIELDCOUNT \
-p fieldlength=$FIELDLENGTH \
-p maxscanlength=$MAXSCANLENGTH \
-p readproportion=$READPROPORTION \
-p insertproportion=$INSERTPROPORTION \
-p scanproportion=$SCANPROPORTION \
-p inputproportion=$INPUTPROPORTION \
-p updateproportion=$UPDATEPROPORTION \
-p measurementtype=timeseries -s
else
echo "Loading workload"
python $YCSB_HOME/bin/ycsb load $DBTYPE -p workload=$WORKLOAD \
-p hosts=$HOSTS \
-p threadcount=$THREADS \
-p target=$TARGET \
-p recordcount=$RECORDCOUNT \
-p operationcount=$OPERATIONCOUNT \
-p insertcount=$INSERTCOUNT \
-p maxexecutiontime=$MAXEXECUTIONTIME \
-p fieldcount=$FIELDCOUNT \
-p fieldlength=$FIELDLENGTH \
-p maxscanlength=$MAXSCANLENGTH \
-p readproportion=$READPROPORTION \
-p insertproportion=$INSERTPROPORTION \
-p scanproportion=$SCANPROPORTION \
-p inputproportion=$INPUTPROPORTION \
-p updateproportion=$UPDATEPROPORTION \ 
-p measurementtype=timeseries -s
fi


