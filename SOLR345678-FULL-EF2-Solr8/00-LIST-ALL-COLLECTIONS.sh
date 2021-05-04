#!/bin/bash

source ./_setcol.sh

echo "****"
echo "* Listing all collections through Solr endpoint: $solradminurl"
echo "****"

solr_cmd="$solradminurl/collections?action=list"
#col_exists=`wget $opt_authenticate -q "$solr_cmd" -O - \
#    | python -c "import sys, json; cols=json.load(sys.stdin)['collections']; print '$solrcol' in cols" `

echo wget $opt_authenticate -q \"$solr_cmd\" -O -

