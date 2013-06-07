SoPeCoYCSB
==========

Automated Yahoo Cloud Serving Benchmark System

Prerequisites:Apache Cassandra, Apache Derby, Apache TomCat

How to Start

Note: $DERBY_HOME, $TOMCAT_HOME, $CASSANDRA_HOME must be initialized

1. Start Derby DataBase in Terminal
 java -jar $DERBY_HOME/lib/derbyrun.jar server start

2. Start TomCat 
./$TOMCAT_HOME/bin/startup.sh

3. Start Cassandra
./$CASSANDRA_HOME/bin/cassandra

4. Visit SoPeCo Server URL. Default is http://localhost:8089/sopeco

5. Use WEB-UI to set parameters for experiments and run experiments
