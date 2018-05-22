#!/bin/bash

cat errors.txt | awk '{print $3}' | sed 's/b\([0-9]\)/\$b\1/g' | sed 's/\"//g' > "/tmp/errors-$$.txt"

for f in `cat /tmp/errors-$$.txt` ; do
    id_ext=${f##*/}
    id=${id_ext%.json.bz2}
    echo "**** Converting: $id" >&2
    ./id-to-rsync-id.py $id
done



