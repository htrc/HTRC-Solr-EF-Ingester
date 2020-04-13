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


wget $opt_authenticate "$solradminurl/collections?action=CREATE&name=$solrcol&numShards=$solrShardCount&replicationFactor=$solrReplCount&collection.configName=$solrconfig" -O -
