### TiDB
#### Set up testing
```shell
mkdir tidb_test
git clone https://github.com/JZuming/TxCheck.git
cp TxCheck/script/tidb/* tidb_test
cd tidb_test 
./build_docker.sh # build tidb docker image
docker exec -it txcheck-tidb-container bash # enter the docker container
sh test.sh
exit # exit the docker container
```
#### Check testing status
```shell
docker exec -it txcheck-tidb-container bash # enter docker container 
ls test/found_bugs # show the found bug-triggering test cases, each test case is stored in a directory
cat test/nohup.out # check the testing log
exit # exit the docker container
```
#### Stop testing
```shell
docker stop $(docker ps -a --format "table {{.Names}}" | grep txcheck-tidb-container) # stop all docker containers
```
#### Clean test results
```shell
docker rm $(docker ps -a --format "table {{.Names}}" | grep txcheck-tidb-container) # remove all docker containers