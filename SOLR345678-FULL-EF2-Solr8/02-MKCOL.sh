#!/bin/bash

source ./_setcol.sh

echo "****"
echo "* Creating collection:   $solrcol"
echo "* With Solr configset:   $solrconfig"
echo "* Through Solr endpoint: $solradminurl"
echo "****"

echo "#"
echo "# First checking if collection '$solrcol' already exists: "

solr_cmd="$solradminurl/collections?action=list"
col_exists=`wget $opt_authenticate -q "$solr_cmd" -O - \
    | python -c "import sys, json; cols=json.load(sys.stdin)['collections']; print '$solrcol' in cols" `

if [ "x$col_exists" != "x" ] ; then
    # running command produced a result
    if [ "$col_exists" = "True" ] ; then
	echo "#  Exists => Exiting as not possible to add a Solr collection when one already exists"
	exit -1
    else
	echo "#  Does not exist => Proceeding to add $solrcol"
    fi
fi

echo ""



# Useful details on controling shard location at:
#   https://sematext.com/blog/handling-shards-in-solrcloud/

# e.g. control shard create at replication 1
#
# curl 'localhost:8983/solr/admin/collections?action=CREATE&name=manualplace&numShards=2&replicationFactor=1&createNodeSet=172.19.0.3:8983_solr,172.19.0.4:8983_solr'

# then systmatically replicate a shard (one at a time) on another solr node
# curl 'localhost:8983/solr/admin/collections?action=ADDREPLICA&collection=manualreplication&shard=shard1&node=172.19.0.3:8983_solr'


createNodeSet="createNodeSet=solr3:9983_solr"
createNodeSet="$createNodeSet,solr6:9983_solr"
createNodeSet="$createNodeSet,solr3:9985_solr"
createNodeSet="$createNodeSet,solr6:9985_solr"
createNodeSet="$createNodeSet,solr3:9987_solr"
createNodeSet="$createNodeSet,solr6:9987_solr"
createNodeSet="$createNodeSet,solr3:9989_solr"
createNodeSet="$createNodeSet,solr6:9989_solr"
createNodeSet="$createNodeSet,solr4:9983_solr"
createNodeSet="$createNodeSet,solr7:9983_solr"
createNodeSet="$createNodeSet,solr4:9985_solr"
createNodeSet="$createNodeSet,solr7:9985_solr"
createNodeSet="$createNodeSet,solr4:9987_solr"
createNodeSet="$createNodeSet,solr7:9987_solr"
createNodeSet="$createNodeSet,solr4:9989_solr"
createNodeSet="$createNodeSet,solr7:9989_solr"
createNodeSet="$createNodeSet,solr5:9983_solr"
createNodeSet="$createNodeSet,solr8:9983_solr"
createNodeSet="$createNodeSet,solr5:9985_solr"
createNodeSet="$createNodeSet,solr8:9985_solr"
createNodeSet="$createNodeSet,solr5:9987_solr"
createNodeSet="$createNodeSet,solr8:9987_solr"
createNodeSet="$createNodeSet,solr5:9989_solr"
createNodeSet="$createNodeSet,solr8:9989_solr"


wget $opt_authenticate "$solradminurl/collections?action=CREATE&name=$solrcol&numShards=$solrShardCount&replicationFactor=1&$createNodeSet&collection.configName=$solrconfig&property.solr.cloud.client.stallTime=119999" -O -

