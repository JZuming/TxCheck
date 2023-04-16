### MariaDB
#### Set up testing
```shell
mkdir mariadb_test
git clone https://github.com/JZuming/TxCheck.git
cp TxCheck/script/mariadb_test/* mariadb_test
cd mariadb_test 
./build_docker.sh # build mariadb docker image
./run_test.sh 4 # build 4 mariadb docker containers and set up testing, the arugment is the number of containers
```
#### Check testing status
```shell
# if "./run_test.sh 4", there are four docker containers
# check the bug-finding status in container 1
docker exec -it txcheck-mariadb-container-1 bash # enter docker container 1
ls found_bugs # show the found bug-triggering test cases, each test case is stored in a directory
cat nohup.out # check the testing log
exit # exit the docker container
```
#### Stop testing
```shell
docker stop $(docker ps -a --format "table {{.Names}}" | grep txcheck-mariadb-container) # stop all docker containers
```
#### Clean test results
```shell
docker rm $(docker ps -a --format "table {{.Names}}" | grep txcheck-mariadb-container) # remove all docker containers