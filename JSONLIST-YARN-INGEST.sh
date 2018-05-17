#!/bin/bash

solr_col=${1:-TMP-faceted-htrc-fictsample-ef20}
solr_config=${2:-htrc_configs}

SOLR_NODES_ARRAY=($SOLR_NODES)
solr_node=${SOLR_NODES_ARRAY[0]}
solr_host=${solr_node%.*}


solr_endpoint="http://$solr_host/solr/admin"
solr_cmd="$solr_endpoint/collections?action=list"

echo ""
echo "Checking if collection '$solr_col' exists: "
col_exists=`wget -q "$solr_cmd" -O - \
    | python -c "import sys, json; cols=json.load(sys.stdin)['collections']; print '$solr_col' in cols" `

if [ "x$col_exists" != "x" ] ; then
    # running command produced a result
    if [ "$col_exists" = "True" ] ; then
	echo "  Exists"
    else
	echo "  Does not exist."
	echo ""
	echo "You can create the collection through the Solr admin interface, for example:"
	echo "  wget \"$solr_endpoint/collections?action=CREATE&name=$solr_col&numShards=4&replicationFactor=1&collection.configName=$solr_config\" -O -"
	echo ""
	echo "Collectcion '$solr_col' could not be found on Solr endpoint '$solr_endpoint'. Exiting" >&2
	exit -1
    fi
else
    echo "Something went wrong running the command:" >&2
    echo " $solr_cmd" >&2
    echo "Exiting" >&2
    exit -1
fi



nohup_cmd="./SCRIPTS-CWD/JSONLIST-RUN-YARN-SPARK.sh pair-tree-annika-1k-fiction-vol-ids.txt $solr_col"
echo ""
echo "Launching nohup cmd:"
echo "  $nohup_cmd"

nohup $nohup_cmd &

#nohup ./SCRIPTS-CWD/JSONLIST-RUN-YARN-SPARK.sh \
#      pair-tree-annika-1k-fiction-vol-ids.txt $solr_col &
