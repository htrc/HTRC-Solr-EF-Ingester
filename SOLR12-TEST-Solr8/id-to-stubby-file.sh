
inst_id="$1"

inst=${inst_id%%.*}
id=${inst_id#*.}

id_file_safe=`echo $id | sed 's/\//=/g' | sed 's/:/+/g' | sed 's/\./,/g'`

stubby_file_safe="${id_file_safe:0:1}"
for ((i=3; i<${#id_file_safe}; i=i+3))
do
    stubby_file_safe="${stubby_file_safe}${id_file_safe:$i:1}"
done

inst_stubby_file="$inst/$stubby_file_safe/$inst.$id_file_safe.json.bz2"

echo $inst_stubby_file
