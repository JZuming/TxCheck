# TxCheck

## Description

TxCheck is a tool for finding transctional bugs in database management systems. It uses SQL-level instrumentation to capture statement-level dependencies and construct transactional oracle to find bugs in transaction supports of DBMSs. We implemented TxCheck on the top of SQLsmith (https://github.com/anse1/sqlsmith).

## Supported DBMSs
- MySQL
- MariaDB
- TiDB

## Build TxCheck

### Building and Testing in Docker (Recommanded)
- [Test MySQL](./docs/mysql_test.md)
- [Test MariaDB](./docs/mariadb_test.md)
- [Test TiDB](./docs/tidb_test.md)

### Building in Debian

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
The bugs found are stored in the directory `found_bugs`. TxCheck only support testing local database engines now.

The following options are supported:

| Option | Description |
|----------|----------|
| `--mysql-db` | target MySQL database | 
| `--mysql-port` | MySQL server port number | 
| `--mariadb-db` | target MariaDB database |
| `--mariadb-port` | Mariadb server port number |
| `--tidb-db` | target TiDB database |
| `--tidb-port` | TiDB server port number |
| `--output-or-affect-num` | generated statement should output or affect at least a specific number of rows |
| `--reproduce-sql` | a SQL file recording the executed statements (needed for reproducing)|
| `--reproduce-tid` | a file recording the transaction id of each statement (needed for reproducing)|
| `--reproduce-usage` | a file recording the type of each statement (needed for reproducing)|
| `--reproduce-backup` | a backup file (needed for reproducing)|
| `--min` | minimize the bug-triggering test case|

***Note***

Both target database and the server port number should be specified (e.g., when testing MySQL or reproducing a bug in MySQL, `--mysql-db` and `--mysql-port` should be specified).

The options `--reproduce-sql`, `--reproduce-tid`, `--reproduce-usage`, and `--reproduce-backup` should be specified when TxCheck is used to reproduce a found bug. The files used are the files stored in the directory `found_bugs`. The option `--min` can work only when the options `--reproduce-sql`, `--reproduce-tid`, and `--reproduce-usage` and `--reproduce-backup` are specified.

## Found bugs
- [Reported bugs in MySQL](./docs/mysql_bugs.md)
- [Reported bugs in MariaDB](./docs/mariadb_bugs.md)
- [Reported bugs in TiDB](./docs/tidb_bugs.md)
