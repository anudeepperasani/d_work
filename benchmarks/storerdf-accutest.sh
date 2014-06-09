#!/bin/bash
DATA_DIR=`pwd`/data/lubm
num=$1

CP="`pwd`/target/classes/"
export MAVEN_OPTS="-Xms1g -Xmx2g"

rm -rf $DATA_DIR/cassandra
export CASSANDRA_CONF="/Users/Deepu/Documents/maven/turbulence/lubm-cassandra-conf/"
~/Documents/maven/turbulence/softwares/apache-cassandra-2.0.0/bin/cassandra -p ./cpid
sleep 3
CPID=`cat cpid`
mvn exec:java -Dexec.mainClass=com.turbulence.Turbulence&
PID=$!
sleep 3
cat /Users/Deepu/Downloads/datasets/lubm/u0xml/12/university.xml.rdf | curl -v -X POST -H "Content-Type: application/xml" http://localhost:5000/api/1/store_data --data-binary @-
#cat /Users/nikhilmarathe/Downloads/datasets/lubm/u0xml/12/data.owl | curl -v -X POST -H "Content-Type: application/xml" http://localhost:5000/api/1/store_data --data-binary @-
kill -TERM $PID
kill -TERM $CPID
