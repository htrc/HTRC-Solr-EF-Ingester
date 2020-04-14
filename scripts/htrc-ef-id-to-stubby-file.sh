#!/bin/bash

if [ $# != 1 ] ; then
    prog_name=${0##*/}
    script_dir=${0%/*}
    echo "Usage: ./$prog_name 'hathitrust-id'" >&2
    echo "(or consider adding '$script_dir' to your PATH)" >&2
    exit 1
fi

inst_id="$1"

inst=${inst_id%%.*}
id=${inst_id#*.}

# take care of characters that need to be mapped to file-safe equivalents
id_file_safe=`echo $id | sed 's/\//=/g' | sed 's/:/+/g' | sed 's/\./,/g'`

# chop up the id!
stubby_file_safe="${id_file_safe:0:1}"
for ((i=3; i<${#id_file_safe}; i=i+3)) ; do
    stubby_file_safe="${stubby_file_safe}${id_file_safe:$i:1}"
done

# put the various parts back together
inst_stubby_file="$inst/$stubby_file_safe/$inst.$id_file_safe.json.bz2"

echo $inst_stubby_file
