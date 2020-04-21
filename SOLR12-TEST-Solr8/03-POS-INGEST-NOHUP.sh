#!/bin/bash

source ./_setcol.sh

echo "****"
echo "* Preparing to ingest to collection: $solrcol"
echo "****"
echo ""

export OPT_WGET_AUTHENTICATE=$opt_authenticate

pushd ..
#./JSONLIST-YARN-INGEST-TO-SOLR-ENDPOINT.sh $solrbaseurl $solrcol $solrconfig $solrShardCount /user/dbbridge/fict-subset-pairtree-ids.txt
./JSONLIST-YARN-INGEST-TO-SOLR-ENDPOINT.sh $solrbaseurl $solrcol $solrconfig $solrShardCount /user/dbbridge/fict-subset-stubby-ids.txt
popd
