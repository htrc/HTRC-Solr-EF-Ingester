#!/bin/bash

SOLR_NODES_ARRAY=($SOLR_NODES)
solr_host_with_port=${SOLR_NODES_ARRAY[0]}
solr_host=${solr_host_with_port%:*}

echo ""
echo "Looking up Solr configset schemas on $solr_host"

ssh $solr_host 'cd $HTRC_EF_NETWORK_HOME ; echo ; ls conf/solr7*/' # include blank line to break up diplay

