#!/bin/sh

export ASAN_OPTIONS=detect_leaks=0
nohup /home/mysql/TxCheck/transfuzz --mariadb-db=testdb --mariadb-port=3306 --output-or-affect-num=1 &
