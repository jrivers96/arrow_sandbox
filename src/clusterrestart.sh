#!/bin/bash
#Quick restart script for dev use
iquery -aq "unload_library('pull')" > /dev/null 2>&1
set -e

DBNAME="poliocough_16p9"
#This is easily sym-linkable: ~/scidb
SCIDB="/opt/scidb/16.9"
SCIDB_INSTALL=$SCIDB
export SCIDB_THIRDPARTY_PREFIX="/opt/scidb/16.9"

mydir=`dirname $0`
pushd $mydir
make SCIDB=$SCIDB_INSTALL

scidb.py stopall $DBNAME 
sudo cp libpull.so ${SCIDB_INSTALL}/lib/scidb/plugins/
sudo scp libpull.so scidb@10.0.20.184:${SCIDB_INSTALL}/lib/scidb/plugins/
sudo scp libpull.so scidb@10.0.20.187:${SCIDB_INSTALL}/lib/scidb/plugins/
sudo scp libpull.so scidb@10.0.20.186:${SCIDB_INSTALL}/lib/scidb/plugins/


scidb.py startall $DBNAME 

iquery -aq "load_library('pull')"
