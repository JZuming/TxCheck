#!/bin/sh

export ASAN_OPTIONS=detect_leaks=0
nohup /home/mysql/TxCheck/transfuzz --mysql-db=testdb --mysql-port=3306 --output-or-affect-num=1 &
