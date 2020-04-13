#!/bin/bash

source ./_setcol.sh

echo "****"
echo "* Pinging collection:    $solrcol"
echo "* Through Solr endpoint: $solradminurl"
echo "****"


solr_cmd="$solradminurl/collections?action=list"
col_exists=`wget $opt_authenticate -q "$solr_cmd" -O - \
    | python -c "import sys, json; cols=json.load(sys.stdin)['collections']; print '$solrcol' in cols" `

if [ "x$col_exists" != "x" ] ; then
    # running command produced a result
    if [ "$col_exists" = "True" ] ; then
	echo "#  Exists!"
    else
	echo "# Does NOT exist!"
    fi
else
    echo "# Does NOT exist!"
fi
echo "****"

