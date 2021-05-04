#!/bin/bash

source ./_setcol.sh

echo "****"
echo "* Replicating collection:   $solrcol"
echo "* With Solr configset:      $solrconfig"
echo "* Through Solr endpoint:    $solradminurl"
echo "****"


# Useful details on controling shard location at:
#   https://sematext.com/blog/handling-shards-in-solrcloud/

# e.g. control shard create at replication 1
#
# curl 'localhost:8983/solr/admin/collections?action=CREATE&name=manualplace&numShards=2&replicationFactor=1&createNodeSet=172.19.0.3:8983_solr,172.19.0.4:8983_solr'

# then systmatically replicate a shard (one at a time) on another solr node
# curl 'localhost:8983/solr/admin/collections?action=ADDREPLICA&collection=manualreplication&shard=shard1&node=172.19.0.3:8983_solr'

# https://opensource.com/article/18/5/you-dont-know-bash-intro-bash-arrays
#for i in ${!allThreads[@]}; do
#    ./pipeline --threads ${allThreads[$i]}
#done


#in_order_shards=(4 9 7 13 18 16 23 6 24 21 15 5 20 17 3 19 12 8 22 11 1 14 10 2)
#in_order_shards=(9 7 13 18 16 23 6 24 21 15 5 20 17 3 19 12 8 22 11 1 14 10 2)
in_order_shards=(7 13 18 16 23 6 24 21 15 5 20 17 3 19 12 8 22 11 1 14 10 2)

#dst_nodes=( \
#    solr6:9984_solr solr6:9986_solr solr6:9988_solr solr6:9990_solr \

dst_nodes=( \
    solr6:9988_solr solr6:9990_solr \
    solr7:9984_solr solr7:9986_solr solr7:9988_solr solr7:9990_solr \
    solr8:9984_solr solr8:9986_solr solr8:9988_solr solr8:9990_solr \
    solr3:9984_solr solr3:9986_solr solr3:9988_solr solr3:9990_solr \
    solr4:9984_solr solr4:9986_solr solr4:9988_solr solr4:9990_solr \
    solr5:9984_solr solr5:9986_solr solr5:9988_solr solr5:9990_solr \
)

#date
#echo "**** one time fix: Sleeping for 1 hour ..."
#sleep 3600
#date
    
# This script does the latter part of the 2-step process detailed above
#i=1
#ios_pos=0

#for node in \
#    solr3:9984_solr solr6:9984_solr solr3:9986_solr solr6:9986_solr solr3:9988_solr solr6:9988_solr solr3:9990_solr solr6:9990_solr \
#    solr4:9984_solr solr7:9984_solr solr4:9986_solr solr7:9986_solr solr4:9988_solr solr7:9988_solr solr4:9990_solr solr7:9990_solr \
#    solr5:9984_solr solr8:9984_solr solr5:9986_solr solr8:9986_solr solr5:9988_solr solr8:9988_solr solr5:9990_solr solr8:9990_solr ; do

for i_pos in ${!dst_nodes[@]} ; do

    node=${dst_nodes[$i_pos]}
    shard_i=${in_order_shards[$i_pos]}
    
    echo wget $opt_authenticate "$solradminurl/collections?action=ADDREPLICA&collection=$solrcol&shard=shard$shard_i&node=$node" -O -
    wget $opt_authenticate "$solradminurl/collections?action=ADDREPLICA&collection=$solrcol&shard=shard$shard_i&node=$node" -O -

    echo "========"
    date
#    echo "Sleeping for 1 hour ..."
#    sleep 3600
    echo "Sleeping for 1 hour 15 mins ..."
    sleep 4500
    date
    echo "========"


#    i=$((i+1))
#    ios_pos=$((ios_pos+1))
done

