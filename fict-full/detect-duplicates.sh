
echo "Number of IDs in published list"
wc -l fict-all-ids.txt  
echo

echo "Duplicate IDs"
sort fict-all-ids.txt  | uniq -d 
echo

echo "Generating de-duplicated rsync-ids.txt ..."
cat rsync-ids-errors-removed.txt fixed-rsync-ids.txt | sort | uniq > rsync-ids.txt
echo "done"
echo

echo "Number of IDs in rsync-ids.txt:"
wc -l rsync-ids.txt
echo
