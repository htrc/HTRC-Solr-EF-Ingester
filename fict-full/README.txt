
# Downloads TSV file and then use awk to extract ids as separate text file
  ./download-fict-all.sh

# The following takes a while to run
  ./ids-to-rsync-ids.sh > rsync-ids-with-errors.txt

# The following generates errors as a result of some ids not correctly having '$b' in them
  ./download-rsync-ids.sh rsync-ids-with-errors.txt 2>errors.txt
  
Edit errors.txt to remove empty line at top, and rsync finished line at bottom

  ./fix-up-errors.sh > fixed-rsync-ids.txt
  ./download-rsync-ids.sh fixed-rsync-ids.txt

# Copy the json files into HDFS
  hdfs dfs -cp json-files /user/dbbridge/.
  
# Generate clean list of rsync-ids
./detect-duplicates.sh
