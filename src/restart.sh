#!/bin/bash
#Quick restart script for dev use
iquery -aq "unload_library('pull')" > /dev/null 2>&1
set -e

DBNAME="test_dbname"
#This is easily sym-linkable: ~/scidb

SCIDB_INSTALL=$SCIDB
export SCIDB_THIRDPARTY_PREFIX="/opt/scidb/16.9"

mydir=`dirname $0`
pushd $mydir
make SCIDB=$SCIDB_INSTALL

scidb.py stopall $DBNAME 
sudo cp libpull.so ${SCIDB_INSTALL}/lib/scidb/plugins/
scidb.py startall $DBNAME 

iquery -aq "load_library('pull')"
