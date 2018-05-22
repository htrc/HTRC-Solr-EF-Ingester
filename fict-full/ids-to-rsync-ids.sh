#!/bin/bash

for f in `cat fict-all-ids.txt` ; do
    ./id-to-rsync.py "$f" ; 
done

