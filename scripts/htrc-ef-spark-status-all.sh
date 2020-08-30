#!/bin/bash

function JPSFilter() {

    jps_host="$1"    
    jps_filter="$2"
    
    echo "Checking $jps_host using 'jps' for $jps_filter process ..."
    jps_master_proc_output=$(ssh $jps_host "jps |& grep $jps_filter")
    jps_master_proc_count=$(echo "$jps_master_proc_output" | sed '/^\s*$/d' | wc -l)
    
    
    if [ $jps_master_proc_count = "1" ] ; then
	echo "... OK"
    else
	echo "... Failed"
	echo "Unexpected output from 'jps' encountered" >&2
	echo "----" >&2
	if [ "x$jps_master_proc_output" = "x" ] ; then
	    echo "<no matching output resulted from jps>" >&2
	else
	    echo "$jps_master_proc_output" >&2
	fi    
	echo "----" >&2    
    fi
}


JPSFilter $SPARK_MASTER_HOST "Master"

for h in $SPARK_SLAVE_HOSTS; do
    JPSFilter $h "Worker"
done


