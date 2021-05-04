#!/bin/bash

source ./_setcol.sh

echo "****"
echo "* Preparing to ingest to collection: $solrcol"
echo "*"
echo "* Key properties controling the Spark-based ingest are in:"
echo "*   ../spark-solr-ef.properties.in"
echo "*---"
egrep -v '^#' ../spark-solr-ef.properties.in | egrep -v '^$'
echo "*---"

echo "****"
echo ""

echo "Pausing for 5 seconds to allow time to review properties"
echo "Press ^C to abort"
sleep 5



echo "****"
echo "* Away to check Apache Spark Cluster has Master and all Slave processing running"
echo "****"
echo ""
htrc-ef-spark-status-all.sh

echo "Pausing for 5 seconds before submitting job to Spark"
echo "Press ^C to abort"
sleep 5


export OPT_WGET_AUTHENTICATE=$opt_authenticate

pushd ..
pwd
nohup ./SCRIPTS-CWD/FULL-RUN-YARN-SPARK-EF2.sh $solrcol &
popd
