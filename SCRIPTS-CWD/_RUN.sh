#!/bin/bash 

# To work, the follow bash variables need to have been set:
#
#  seq_file (output_dir optional)
#
# Or:
#
#  json_filelist input_dir (output_dir optional)
#
# Typically done through running a wrapper script, such as:
#
#  FULL-RUN-YARN-SPARK.sh

HTRC_SOLR_EF_INGESTER_HOME=`pwd`

show_usage=1
class_mode=""
if [ "x$seq_file" != "x" ] ; then
    show_usage=0
    class_mode="seq"
else

    
    if [ "x$json_filelist" != "x" ] ; then
	class_mode="json"
    fi

    if [ "x$input_dir" != "x" ] ; then
	if [ $class_mode = "json" ] ; then
	    show_usage=0
	fi
    fi

fi

if [ $show_usage = "1" ] ; then
    echo "_RUN.bash: Failed to set 'seq_file' or 'input_dir json_filelist" 1>&2
    exit 1
fi


#if [ "x$output_dir" = "x" ] ; then
#    echo "_RUN.bash: Failed to set 'output_dir'" 1>&2
#    exit
#fi

run_jps=0
run_jps_daemons=""
run_jps_daemons_suffix="daemon"
using_hdfs=0

if [ "$class_mode" = "seq" ] ; then
  if [ "x${seq_file##hdfs://*}" = "x" ] || [ "x${output_dir##hdfs://*}" = "x" ] ; then
    # Evidence of running command over HDFS
    run_jps=1
    run_jps_daemons="Spark"
    using_hdfs=1
  fi
fi

if [ "$class_mode" = "json" ] ; then
  if [ "x${input_dir##hdfs://*}" = "x" ] || [ "x${output_dir##hdfs://*}" = "x" ] ; then
    # Evidence of running command over HDFS
    run_jps=1
    run_jps_daemons="Spark"
    using_hdfs=1
  fi
fi

if [ "x${master_opt##--master spark://*}" = "x" ] ; then
    # Evidence of running command submitted to Spark cluster
    run_jps=1
    if [ "x$run_jps_daemons" != "x" ] ; then
        run_jps_daemons="$run_jps_daemons and Hadoop"
	run_jps_daemons_suffix="daemons"
    else
        run_jps_daemons="Hadoop"
    fi
fi

if [ "$run_jps" = "1" ] ; then
  echo
  echo "****"
  echo "* Checking for $run_jps_daemons $run_jps_daemons_suffix, by running 'jps':"
  echo "****"
  jps | egrep -v " Jps$" |  sed 's/^/* /g' \
    | sed 's/ Master/ [Spark] Master/' \
    | sed 's/ NameNode/ [HDFS]  NameNode/' \
    | sed 's/ SecondaryNameNode/ [HDFS]  SecondaryNameNode/'

  echo "****"
  echo "* Done"
  echo "****"
  echo

  sleep 1
fi

if [ "$using_hdfs" = "1" ] ; then
    if [ "x$output_dir" != "x" ] ; then
      hadoop fs -test -d "$output_dir"

    if [ $? != 0 ] ; then
      echo "Creating directory:"
      echo "  $output_dir"
    fi
  fi
fi

if [ "x$classmain" = "x" ] ; then
    classmain="org.hathitrust.extractedfeatures.ProcessForSolrIngest"
fi    

self_contained_jar=target/htrc-ef-ingest-0.9-jar-with-dependencies.jar
cmd="spark-submit --class $classmain $master_opt $self_contained_jar"

if [ "$classmain" = "org.hathitrust.extractedfeatures.ProcessForSolrIngest" ] || [ "$classmain" = "org.hathitrust.extractedfeatures.ProcessForSolrIngestJSONFilelist" ] ; then
  if [ "x$solr_base_url" != "x" ] ; then
      cmd="$cmd --solr-base-url $solr_base_url"
  fi

  if [ "x$output_dir" != "x" ] ; then
    cmd="$cmd --output-dir $output_dir"
  fi
fi


if [ "$classmain" = "org.hathitrust.extractedfeatures.ProcessForWhitelist" ] ; then
  if [ "x$output_dir" != "x" ] ; then
    cmd="$cmd --output-dir $output_dir"
  fi
fi

    
#
#cmd="$cmd --verbosity 2"
#

# Generate properties file for this invokation from template file
# 

spark_solr_ef_propfile="$HTRC_SOLR_EF_INGESTER_HOME/spark-solr-ef.properties"
spark_solr_ef_propfile_in="$spark_solr_ef_propfile.in"

# Replace this copy with a 'sed' set of substitues
#/bin/cp "$spark_solr_ef_propfile_in" "$spark_solr_ef_propfile"

cat "$spark_solr_ef_propfile_in" \
    | sed "s%@htrc-solr-ef-ingester-home@%$HTRC_SOLR_EF_INGESTER_HOME%g" \
    > "$spark_solr_ef_propfile"

echo "****"
echo "* Cloud node Solr endpoint operation"

solr_cloud_endpoint_operation_output=`egrep "^wcsa-ef-ingest.solr-cloud-nodes" "$spark_solr_ef_propfile"`
echo "=="
if [ "x$solr_cloud_endpoint_operation_output" = "x" ] ; then
    echo "<empty> => this means the command-line provided --solr-base-url='$solr_base_url' will be used"
else
    echo $solr_cloud_endpoint_operation_output
fi
echo "=="


cmd="$cmd --properties $spark_solr_ef_propfile"


if [ "$class_mode" = "seq" ] ; then
    ##cmd="$cmd --properties ef-solr.properties $seq_file $*"
    ##cmd="$cmd --properties /homea/dbbridge/extracted-features-solr/solr-ingest/ef-solr.properties $seq_file $*"
    #cmd="$cmd --properties /homea/dbbridge/HTRC-Solr-EF-Setup/HTRC-Solr-EF-Ingester/ef-solr.properties $seq_file $*"
    cmd="$cmd $seq_file $*"
else
    ##cmd="$cmd --properties ef-solr.properties $input_dir $json_filelist $*"
    ##cmd="$cmd --properties /homea/dbbridge/extracted-features-solr/solr-ingest/ef-solr.properties $input_dir $json_filelist $*"
    #cmd="$cmd --properties /homea/dbbridge/HTRC-Solr-EF-Setup/HTRC-Solr-EF-Ingester/ef-solr.properties $input_dir $json_filelist $*"
    cmd="$cmd  $input_dir $json_filelist $*"
fi

echo "****"
echo "* Launching:"
echo "*   $cmd"
echo "****"

if [ "$run_jps" = "1" ] ; then
  echo "* Monitor progress on Spark cluster through:"
  echo "*   http://$SPARK_MASTER_HOST:8080/"
  echo "* Or alternatively through YARN (if SOCKETD ssh + browser network settings setup)"
  echo "*   http://gchead:8088/cluster/apps"
  echo "****"
fi
echo

#echo "!!!!"
#echo "! Temporarily supressing the running of the command!!!"
#echo "!!!!"

sleep 5
$cmd

