# TxCheck

## Description

TxCheck is a tool for finding transctional bugs in database management systems. It uses SQL-level instrumentation to capture statement-level dependencies and construct transactional oracle to find bugs in transaction supports of DBMSs. We implemented TxCheck on the top of SQLsmith (https://github.com/anse1/sqlsmith).

## Supported DBMSs
- MySQL
- MariaDB
- TiDB

## Build TxCheck and Test DBMSs
- [Test MySQL](./docs/mysql_test.md)
- [Test MariaDB](./docs/mariadb_test.md)
- [Test TiDB](./docs/tidb_test.md)

## Found bugs
- [Reported bugs in MySQL](./docs/mysql_bugs.md)
- [Reported bugs in MariDB](./docs/mariadb_bugs.md)
- [Reported bugs in TiDB](./docs/tidb_bugs.md)
