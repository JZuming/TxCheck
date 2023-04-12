FROM ubuntu:20.04 
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y apt-utils
RUN apt-get install -y build-essential python3-dev automake git flex bison libglib2.0-dev libpixman-1-dev python3-setuptools
RUN apt-get install -y vim cmake curl wget gdb

# install mysql
RUN apt-get install -y libssl-dev libncurses5-dev
RUN useradd -ms /bin/bash mysql
USER  mysql
WORKDIR /home/mysql
RUN wget https://github.com/mysql/mysql-server/archive/refs/tags/mysql-8.0.28.tar.gz
RUN tar -zxf mysql-8.0.28.tar.gz
RUN mv mysql-server-mysql-8.0.28 mysql_source/
WORKDIR /home/mysql/mysql_source
RUN mkdir build
# build mysql
USER  mysql
WORKDIR /home/mysql/mysql_source/build
# RUN cmake -DWITH_ASAN=ON -DWITH_ASAN_SCOPE=ON -DWITH_DEBUG=ON ../ -DDOWNLOAD_BOOST=1 -DWITH_BOOST=/home/mysql
RUN cmake ../ -DDOWNLOAD_BOOST=1 -DWITH_BOOST=/home/mysql
RUN make -j

# install mysql
USER  root
RUN make install
RUN chown -R mysql:mysql /usr/local/mysql/
USER  mysql
WORKDIR /usr/local/mysql/
RUN ./bin/mysqld --initialize-insecure --thread_stack=512k

# get TxCheck
USER  root
RUN apt-get update
RUN apt-get install -y build-essential autoconf autoconf-archive libpqxx-dev libboost-regex-dev libsqlite3-dev
RUN apt-get install -y libmysqlclient-dev
WORKDIR /home/mysql
# COPY dynsql-sqlsmith /home/mysql/dynsql-sqlsmith
RUN git clone https://github.com/JZuming/TxCheck
WORKDIR /home/mysql/TxCheck
RUN autoreconf -if
RUN ./configure
RUN make clean; make -j

USER  root
WORKDIR /home/mysql
ENTRYPOINT bash