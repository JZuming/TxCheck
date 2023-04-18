#!/bin/sh

# set up server
echo "Setting up server ... "
nohup /root/tidb/bin/tidb-server >> server_log 2>&1 &
sleep 5s

# set up test
echo "Setting up test ... "
mkdir test
cd test
pwd
nohup /root/TxCheck/transfuzz --tidb-db=testdb --tidb-port=4000 --output-or-affect-num=1 &
