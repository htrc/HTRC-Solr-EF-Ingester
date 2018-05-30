#!/bin/bash

# Solr cloud is remote to this computer
SOLR_NODES_ARRAY=($SOLR_NODES)
solr_host_with_port=${SOLR_NODES_ARRAY[0]}
solr_host=${solr_host_with_port%:*}

echo ""
echo "Looking up Solr configset schemas on $solr_host"

ssh $solr_host 'echo ; htrc-ef-solr-local-list-configsets.sh' # include blank line to break up diplay

