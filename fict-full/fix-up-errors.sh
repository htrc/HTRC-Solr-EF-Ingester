#!/bin/bash

echo "Generate rsync-ids-errors-removed.txt" >&2
moster_re=`egrep -v "^$" errors.txt | awk '{print $3}' | sed 's/\"//g' | tr '\n' '|' | sed 's/|$//'`
cat rsync-ids-with-errors.txt | egrep -v "$moster_re" > rsync-ids-errors-removed.txt

egrep -v "^$" errors.txt | awk '{print $3}' | sed 's/b\([0-9]\)/\$b\1/g' | sed 's/\"//g' > "/tmp/errors-$$.txt"

for f in `cat /tmp/errors-$$.txt` ; do
    id_ext=${f##*/}
    id=${id_ext%.json.bz2}
    echo "**** Converting: $id" >&2
    ./id-to-rsync-id.py $id
done




