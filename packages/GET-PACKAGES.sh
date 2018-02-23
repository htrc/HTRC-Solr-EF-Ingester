#!/bin/bash

get_jdk="n"
get_maven="n"
get_hadoop="n"
get_spark="y"


if [ $get_jdk = "y" ] ; then
  echo "***"
  echo "Java JDK 1.8"
  echo "***"
  wget http://www.java.net/download/java/jdk8u122/archive/b04/binaries/jdk-8u122-ea-bin-b04-linux-arm64-vfp-hflt-25_oct_2016.tar.gz
  echo "***"
  echo ""
fi

if [ $get_maven = "y" ] ; then
  echo "***"
  echo "Maven 3.3.9"
  echo "***"
  wget http://www-us.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
  echo "***"
  echo ""
fi

if [ $get_hadoop = "y" ] ; then
  echo "***"
  echo "Hadoop 2.7.3"
  echo "***"
  wget http://www-us.apache.org/dist/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
  echo "***"
  echo ""
fi

if [ $get_spark = "y" ] ; then
  echo "***"
  echo "Spark 2.0.1"
  echo "***"
#  wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.1-bin-hadoop2.7.tgz
  wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.1-bin-hadoop2.6.tgz
  echo "***"
  echo ""
fi
