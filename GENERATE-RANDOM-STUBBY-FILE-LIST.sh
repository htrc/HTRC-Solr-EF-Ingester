#!/bin/bash

echo >&2
echo "Selecting $1 random files from ef2-stubby-file-listing.txt" >&2
echo >&2

shuf -n $1 ef2-stubby-file-listing.txt


echo "****" >&2
echo "* You will then probably want to run something like:" >&2
echo "*   htrc-ef2-rsync-download-jsonfiles.sh random10000-stubby-ids.txt json-files-stubby-random$1" >&2
echo "* Followed by:" >&2
echo "*   hdfs dfs -put json-files-stubby-random$1 ." >&2
echo "****" >&2
