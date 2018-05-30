#!/bin/bash

if [ "x$SOLR_HOME" = "x" ] ; then
    # Solr cloud is remote to this computer
    SOLR_NODES_ARRAY=($SOLR_NODES)
    solr_host_with_port=${SOLR_NODES_ARRAY[0]}
    solr_host=${solr_host_with_port%:*}

    echo ""
    echo "Looking up Solr configset schemas on $solr_host"

    # ssh $solr_host 'cd $HTRC_EF_NETWORK_HOME ; echo ; ls conf/solr7*/' # include blank line to break up diplay
    ssh $solr_host 'echo ; htrc-ef-solr-list-configsets.sh' # include blank line to break up diplay
else

    echo ""
    echo "Configsets in $SOLR_HOME:"
    ls "$SOLR_HOME/server/solr/configsets/"

    echo ""
    echo "Configsets/Collections in Zookeeper $ZOOKEEPER_SERVER"
    htrc-ef-zookeeper-cli.sh | htrc-ef-zookeeper-cli-sanitize.awk
    echo ""
    
fi


