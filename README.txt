
----
Introduction
----

Java code for processing HTRC Extracted Feature JSON files, suitable for 
ingesting into Solr.  Designed to be used on a Spark cluster with HDFS. 

----
Setup Proceddure
----

This is Step 2, of a two step setup procedure.

For Step 1, see:

  http://svn.greenstone.org/other-projects/hathitrust/vagrant-spark-hdfs-cluster/trunk/README.txt

*Assumptions*

  * You have 'svn' and 'mvn' on your PATH

----
Step 2
----

1. Check HDFS and Spark Java daemon processes are running:

    jps

Example output:

    19212 NameNode
    19468 SecondaryNameNode
    19604 Master
    19676 Jps

[[
  Starting these processes was previously covered in Step 1, but in brief,
  after formatting the disk with:

    hdfs namenode -format

  The daemons are started with:
  
    start-dfs.sh
    spark-start-all.sh

  The latter is an alias defined by Step 1 provisioning (created to
  avoid the conflict over 'start-all.sh', which both Hadoop and
  Spark define)
]]

2. Acquire some JSON files to process, if not already done so.
   For example:

    ./scripts/PD-GET-FULL-FILE-LIST.sh
    ./scripts/PD-SELECT-EVERY-10000.sh
    ./scripts/PD-DOWNLOAD-EVERY-10000.sh

3. Push these files over to HDFS

    hdfs dfs -mkdir /user
    hdfs dfs -mkdir /user/htrc

    hdfs dfs -put pd-file-listing-step10000.txt /user/htrc/.
    hdfs dfs -put pd-ef-json-files /user/htrc/.

4. Compile the code:

    ./COMPILE.bash

The first time this is run, a variety of Maven/Java dependencies will be 
downloaded.

5. Run the code on the cluster:

  ./RUN.bash pd-ef-json-filelist-10000.txt


If running subsequently, remove the saved RDD on HDFS first:

  hdfs dfs -rm -r rdd-solr-json-page-files

