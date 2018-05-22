#!/bin/bash

if [ "x$HTRC_EF_NETWORK_HOME" = "x" ] ; then
    echo "Environment variable HTRC_EF_NETWORK_HOME not set" >&2
    echo "Have you sourced SETUP.bash in the HTRC-Solr-EF-Setup directory?" >&2
    exit -1
fi
