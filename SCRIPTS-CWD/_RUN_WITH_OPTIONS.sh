#!/bin/bash 

# To work, one of the followlines of  bash variables need to have been set:
#
#  $seq_file
#
# Or:
#
#  $json_filelist $input_dir
#
# Or:
#
#  $json_oneliners
#
# Typically done through running a wrapper script, such as:
#
#  ./FULL-RUN-YARN-SPARK.sh


show_usage=1
class_mode=""
if [ "x$seq_file" != "x" ] ; then
    show_usage=0
    class_mode="seq"
else

    if [ "x$json_oneliners" != "x" ] ; then
	show_usage=0
	class_mode="jsoncontent"
    else 
	if [ "x$json_filelist" != "x" ] ; then
	    class_mode="jsonlist"
	fi
	
	if [ "x$input_dir" != "x" ] ; then
	    if [ $class_mode = "jsonlist" ] ; then
		show_usage=0
	    fi
	fi
    fi
fi

if [ $show_usage = "1" ] ; then
    echo "_RUN_WITH_OPTIONS.sh: Failed to set bash variable(s) \$seq_file or \$input_dir \$json_filelist or \$json_oneliners" 1>&2
    exit 1
fi

if [ "x$solr_col" == "x" ] ; then
    echo "_RUN_WITH_OPTIONS.sh: Must have \$solr_col set to determine which Solr collection is used" 1>&2
    exit 1
fi

run_jps=0
run_jps_daemons=""
run_jps_daemons_suffix="daemon"
using_hdfs=0

if [ "$class_mode" = "seq" ] ; then
  if [ "x${seq_file##hdfs://*}" = "x" ] ; then
    # Evidence of running command over HDFS
    run_jps=1
    run_jps_daemons="Spark"
    using_hdfs=1
  fi
fi

if [ "$class_mode" = "jsonlist" ] ; then
  if [ "x${input_dir##hdfs://*}" = "x" ] ; then
    # Evidence of running command over HDFS
    run_jps=1
    run_jps_daemons="Spark"
    using_hdfs=1
  fi
fi

if [ "$class_mode" = "jsoncontent" ] ; then
  if [ "x${json_oneliners##hdfs://*}" = "x" ] ; then
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

if [ "x$classmain" = "x" ] ; then
    echo "Error: Must explicitly set bash variable \$classmain, e.g. org.hathitrust.extractedfeatures.ProcessForSolrIngestJSONFilelist" 1>&2
    echo "       This is to avoid triggering default behaviour that could accidentally wipe out a large-scale index" 1>&2
    exit 1;
fi    

self_contained_jar=target/htrc-ef-ingest-0.9-jar-with-dependencies.jar
cmd="spark-submit --class $classmain $master_opt $self_contained_jar"

#if [ "$classmain" = "org.hathitrust.extractedfeatures.ProcessForSolrIngest" ] || [ "$classmain" = "org.hathitrust.extractedfeatures.ProcessForSolrIngestJSONFilelist" ] ; then
#  if [ "x$solr_base_url" != "x" ] ; then
#      cmd="$cmd --solr-base-url $solr_base_url"
#  fi
#fi

if [ "x$solr_base_url" != "x" ] ; then
    cmd="$cmd --solr-base-url $solr_base_url"
fi


#if [ "$class_mode" = "seq" ] ; then
#    #cmd="$cmd --properties ef-solr.properties $seq_file $*"
#    cmd="$cmd --properties /homea/dbbridge/extracted-features-solr/solr-ingest/ef-solr.properties $seq_file $*"
#else
#    #cmd="$cmd --properties ef-solr.properties $input_dir $json_filelist $*"
#    cmd="$cmd --properties /homea/dbbridge/extracted-features-solr/solr-ingest/ef-solr.properties $input_dir $json_filelist $*"
#fi

#cmd="$cmd --properties ef-solr.properties"
cmd="$cmd --properties /homea/dbbridge/HTRC-Solr-EF-Setup/HTRC-Solr-EF-Ingester/ef-solr.properties"

if [ "x$seq_file" != "x" ] ; then
    cmd="$cmd --seq-file $seq_file"
fi

if [ "x$input_dir" != "x" ] ; then
    cmd="$cmd --input-dir $input_dir"
fi
if [ "x$json_filelist" != "x" ] ; then
    cmd="$cmd --json-filelist $json_filelist"
fi

if [ "x$json_oneliners" != "x" ] ; then
    cmd="$cmd --json-oneliners $json_oneliners"
fi

cmd="$cmd $solr_col $*"

echo "****"
echo "* Lauching:"
echo "*   $cmd"
echo "****"

if [ "$run_jps" = "1" ] ; then
  echo "* Monitor progress on Spark cluster through:"
  echo "*   http://$SPARK_MASTER_HOST:8080/"
  echo "****"
fi
echo
sleep 2

$cmd

