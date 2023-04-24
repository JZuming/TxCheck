# TxCheck

## Description

TxCheck is a tool for finding transactional bugs in database management systems. It uses SQL-level instrumentation to capture statement-level dependencies and construct transactional oracle to find bugs in transaction supports of DBMSs. We implemented TxCheck on the top of [SQLsmith](https://github.com/anse1/sqlsmith).

## Supported DBMSs
- MySQL
- MariaDB
- TiDB

## Video

We provide a video introducing the tool. The video is available in the [artifact](https://zenodo.org/) stored in Zenodo.

## Quick Start or Evaluation (in Docker)

We provide scripts to quickly set up the necessary environments and test specific DBMSs using TxCheck. We recommend you follow the instructions in the scripts to evaluate TxCheck or familiarize yourself with TxCheck.

- [Test MySQL 8.0.28](./docs/mysql_test.md)
- [Test MariaDB 10.8.3](./docs/mariadb_test.md)
- [Test TiDB 5.4.0](./docs/tidb_test.md)

By following the scripts, TxCheck can find the bugs listed in [Found Bugs](#found-bugs) (given enough time). For quick evaluation, you could use the above MySQL script to set up the testing, where TxCheck might find a transactional bug in MySQL 8.0.28 within 15 minutes. 

## Build TxCheck in Debian

```shell
apt-get install -y g++ build-essential autoconf autoconf-archive libboost-regex-dev
git clone https://github.com/JZuming/TxCheck.git
cd TxCheck
autoreconf -if
./configure
make -j
```

## Usage
### Test DBMSs
```shell
# test MySQL
./transfuzz --mysql-db=testdb --mysql-port=3306 --output-or-affect-num=1
# test MariaDB
./transfuzz --mariadb-db=testdb --mariadb-port=3306 --output-or-affect-num=1
# test TiDB
./transfuzz --tidb-db=testdb --tidb-port=4000 --output-or-affect-num=1

# reproduce a found bug in MySQl and minimize the test case
./transfuzz --mysql-db=testdb --mysql-port=3306 \
            --reproduce-sql=final_stmts.sql \
            --reproduce-tid=final_tid.txt \
            --reproduce-usage=final_stmt_use.txt \
            --reproduce-backup=mysql_bk.sql \
            --min
```
The bugs found are stored in the directory `found_bugs`. TxCheck only supports testing local database engines now.

### Supported Options

| Option | Description |
|----------|----------|
| `--mysql-db` | Target MySQL database | 
| `--mysql-port` | MySQL server port number | 
| `--mariadb-db` | Target MariaDB database |
| `--mariadb-port` | Mariadb server port number |
| `--tidb-db` | Target TiDB database |
| `--tidb-port` | TiDB server port number |
| `--output-or-affect-num` | Generated statement should output or affect at least a specific number of rows |
| `--reproduce-sql` | A SQL file recording the executed statements (needed for reproducing)|
| `--reproduce-tid` | A file recording the transaction id of each statement (needed for reproducing)|
| `--reproduce-usage` | A file recording the type of each statement (needed for reproducing)|
| `--reproduce-backup` | A backup file (needed for reproducing)|
| `--min` | Minimize the bug-triggering test case|

***Note***

Both target database and the server port number should be specified (e.g., when testing MySQL or reproducing a bug in MySQL, `--mysql-db` and `--mysql-port` should be specified).

The options `--reproduce-sql`, `--reproduce-tid`, `--reproduce-usage`, and `--reproduce-backup` should be specified when TxCheck is used to reproduce a found bug. The files used are the files stored in the directory `found_bugs`. The option `--min` can work only when the options `--reproduce-sql`, `--reproduce-tid`, `--reproduce-usage` and `--reproduce-backup` are specified.

## Source Code Structure

| Source File | Description |
|----------|----------|
| `instrumentor.cc (.hh)` | SQL-level instrumentation to extract dependency information |
| `dependency_analyzer.cc (.hh)` | Build statement dependency graphs, maintain graph-related meta data (e.g. topological sorting on graphs, graph decycling)
| `transaction_test.cc (.hh)` | Manage the whole transaction-testing procedure, including transaction test-case generation, blocking scheduling, transaction-oracle checking, e.t.c.|
| `general_process.cc (.hh)` | Provide general functionality (e.g., hash functions, result-comparison methonds, SQL statement generation) |
| `dbms_info.cc (.hh)` | Maintain the information of supported DBMSs (e.g., tested db, server port number)
| `transfuzz.cc` | Maintain the program entry |
| `mysql.cc (.hh)` | Provide the functionality related to MySQL |
| `mariadb.cc (.hh)` | Provide the functionality related to MariaDB |
| `tidb.cc (.hh)` | Provide the functionality related to TiDB |
| Others | Similar to the ones in SQLsmith. We support more SQL features in `grammar.cc (.hh)` and `expr.cc (.hh)`|


## Found Bugs
- [Reported bugs in MySQL](./docs/mysql_bugs.md)
- [Reported bugs in MariaDB](./docs/mariadb_bugs.md)
- [Reported bugs in TiDB](./docs/tidb_bugs.md)
