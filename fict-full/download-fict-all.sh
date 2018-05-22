#!/bin/bash

wget "https://www.ideals.illinois.edu/bitstream/handle/2142/45713/FictionWorkset1.tsv?sequence=2&isAllowed=y" -O fict-all.tsv

tail -n +2 fict-all.tsv | awk '{print $1}' > fict-all-ids.txt

