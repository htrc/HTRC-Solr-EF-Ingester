#!/bin/bash

source ./_setcol.sh

echo "****"
echo "* Preparing to ingest to collection: $solrcol"
echo "****"
echo ""

pushd ..
./JSONLIST-YARN-INGEST-TO-SOLR-ENDPOINT.sh $solrbaseurl $solrcol $solrconfig $solrShardCount /user/dbbridge/fict-subset-pairtree-ids.txt
popd
