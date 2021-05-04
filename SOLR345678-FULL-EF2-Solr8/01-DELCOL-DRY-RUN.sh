#!/bin/bash

source ./_setcol.sh

echo "****"
echo "* Deleting collection:   $solrcol"
echo "* Through Solr endpoint: $solradminurl"
echo "****"

echo "#"
echo "# First checking if collection '$solrcol' exists: "

solr_cmd="$solradminurl/collections?action=list"
col_exists=`wget $opt_authenticate -q "$solr_cmd" -O - \
    | python -c "import sys, json; cols=json.load(sys.stdin)['collections']; print '$solrcol' in cols" `


if [ "x$col_exists" != "x" ] ; then
    # running command produced a result
    if [ "$col_exists" = "True" ] ; then
	echo "#  Exists"
    else
	echo "#  Does not exist => No need to delete"
	exit -1
    fi
fi

echo ""

echo "****"
echo "Command to run:"

     
echo "  xxwget $opt_authenticate \"$solradminurl/collections?action=DELETE&name=$solrcol\" -O -"
echo "****"
