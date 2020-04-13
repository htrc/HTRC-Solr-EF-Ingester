#!/bin/bash

source ./_setcol.sh

echo "****"
echo "* Pinging collection:   $solrcol"
echo "* Through Solr endpoint: $solradminurl"
echo "****"


wget $opt_authenticate "$solradminurl/collections?action=list&name=$solrcol" -O -
