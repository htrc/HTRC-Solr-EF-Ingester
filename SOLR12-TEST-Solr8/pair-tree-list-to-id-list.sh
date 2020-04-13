#!/bin/bash

cat $*  | sed 's/^.*\///' | sed 's/\.json\.bz2$//' \
 | sed 's/=/\//g' | sed 's/\+/:/g' | sed 's/,/./g'
