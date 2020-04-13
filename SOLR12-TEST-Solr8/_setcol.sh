
solrconfig=htrc-configs-docvals
solrcol=solr12-$USER-test-$solrconfig

solrShardCount=8
solrReplCount=1

# If dedicated switch to a solr machine not available
# (e.g. at the time of writing solr3-s did not respond to 'pint')
# then need to use full domain name:
# e.g. solr3.ischool.illinois.edu:8983

# If direct connection to Solr Jetty server not possible
# then need to go through public facing URL
# e.g. http://solr1-s/solr8"

# Might even be the case the both of the above need to be combined

solrbaseurl="http://solr1-s:9983/solr"

#solrbaseurl="http://solr1-s:9983/solr"
#solrbaseurl="http://solr1-s:8983/solr"
# solr3-6, robust-solr
#solrbaseurl="http://solr1-s/robust-solr"

solradminurl="$solrbaseurl/admin"


if [ -f _password.in ] && [ ! -f _password ] ; then
    echo "****" >&2
    echo "* The admin UI to the Solr/Jetty server is password protected" >&2
    echo "* Copy _password.in to _password and replace with the 'admin' password for the Solr admin UI" >&2
    echo "****" >&2
    exit -1
else
    solradminuser="admin"
    solradminpass=`cat _password`

    opt_authenticate="--auth-no-challenge --http-user=$solradminuser --http-passwd=$solradminpass"
fi

