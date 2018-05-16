#!/bin/bash

echo "Query for *:*"

wget -O /tmp/query-result.html "http://gc0:8983/solr/htrc-full-ef/select?q=*:*" \
  && cat /tmp/query-result.html

