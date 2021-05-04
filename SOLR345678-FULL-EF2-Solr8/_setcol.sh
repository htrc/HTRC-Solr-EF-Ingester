
solrconfig=htrc-configs-docvals
solrcol=solr345678-faceted-htrc-full-ef2-shards24x2

solrShardCount=24

# Given controlled way of createing collection, this value is no longer used
##solrReplCount=2 

# If hostname mapping to solr3-8 machines mis-configured then resort
# to using IP number instead
#
# There has been some network optimization setup done putting in a
# dedicated switch between gsliscluster1 and the solr 'farm'.  Access
# to these machines through the swithc took th form solr1-s, solr2-s
# etc.
#
# However at the time of writing some bit-rot had set in:
#
#   - The newer solr machines (solr7 & solr8) don't appear to have
#     been added to the dedicated switch
#
#   - The IP values to some of the solr boxes appear to have changed,
#     making some of the entries in /etc/hosts erroneous, and worse
#     result in ssh connections that fail

# As a line of last result, work with the IP numbers directly (rather
# than using fully qualified hostnames) as this makes it possible to
# still pick out IP numbers that operate over any private network that
# is present.


# If dedicated switch to a solr machine not available (e.g. at the
# time of writing solr3-s did not respond to 'pint') then need to use
# full domain name: e.g. solr3.ischool.illinois.edu:8983

# If direct connection to Solr Jetty server not possible then need to
# go through public facing URL e.g. http://solr1-s/solr8"

# Might even be the case the both of the above need to be combined


solrhead=solr3.ischool.illinois.edu:9983

solradminurl="http://solr1-s/robust-solr8/admin"

export SOLR_NODES=$solrhead
echo "Set Environment Variable SOLR_NODES to: $SOLR_NODES"


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

