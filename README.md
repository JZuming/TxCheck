# TxCheck

## Description

TxCheck is a tool for finding transctional bugs in database management systems. It uses SQL-level instrumentation to capture statement-level dependencies and construct transactional oracle to find bugs in transaction supports of DBMSs. We implemented TxCheck on the top of SQLsmith (https://github.com/anse1/sqlsmith).

## Supported DBMSs
- MySQL
- MariaDB
- TiDB

## Build and Run
### MySQL
#### Set up testing
```shell
mkdir mysql_test
git clone https://github.com/JZuming/TxCheck.git
cp TxCheck/script/mysql/* mysql_test
cd mysql_test 
./build_docker.sh # build mysql docker image
./run_test.sh 4 # build 4 mysql docker containers and set up testing, the arugment is the number of containers
```
#### Check testing status
```shell
# if "./run_test.sh 4", there are four docker containers
# check the bug-finding status in container 1
docker exec -it txcheck-mysql-container-1 bash # enter docker container 1
ls found_bugs # show the found bug-triggering test cases, each test case is stored in a directory
cat nohup.out # check the testing log
exit # exit the docker container
```
#### Stop testing
```shell
docker stop $(docker ps -a --format "table {{.Names}}" | grep txcheck-mysql-container) # stop all docker containers
```
#### Clean test results
```shell
docker rm $(docker ps -a --format "table {{.Names}}" | grep txcheck-mysql-container) # remove all docker containers
```
