#!/bin/bash

if [ "$#" != "1" ] ; then
    echo "Usage FULL-SELECT-EVERY-N.sh <num>" >&2
    exit 1
fi


num=$1

sed -n "1~${num}p" full-listing.txt > full-listing-step${num}.txt 
