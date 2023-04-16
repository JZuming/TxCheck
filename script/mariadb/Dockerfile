FROM ubuntu:20.04 
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y apt-utils
RUN apt-get install -y build-essential python3-dev automake git flex bison libglib2.0-dev libpixman-1-dev python3-setuptools
RUN apt-get install -y vim cmake curl wget gdb

# for mariadb
RUN apt-get -y update
RUN apt-get -y install software-properties-common devscripts equivs
RUN apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xF1656F24C74CD1D8
RUN add-apt-repository --update --yes --enable-source 'deb [arch=amd64] http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.3/ubuntu '$(lsb_release -sc)' main'
RUN apt-get -y update
RUN apt-get -y build-dep mariadb-10.3

# add user named mysql
RUN useradd -ms /bin/bash mysql

USER mysql
WORKDIR /home/mysql
RUN git clone --branch mariadb-10.8.3 https://github.com/MariaDB/server.git
RUN mv server/ mariadb/

USER mysql
WORKDIR /home/mysql/mariadb
RUN git submodule update --init --recursive
RUN mkdir /home/mysql/mariadb/build
WORKDIR /home/mysql/mariadb/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Debug
RUN make -j
USER root
RUN git config --global --add safe.directory /home/mysql/mariadb
RUN make install
WORKDIR /usr/local/mysql/
RUN scripts/mysql_install_db
RUN chown -R mysql:mysql /usr/local/mysql/
# add soft link in /usr/lib/ for libmysqlclient.so
USER root
WORKDIR /usr/lib
RUN ln -s /usr/local/mysql/lib/libmysqlclient.so libmysqlclient.so

# get TxCheck
USER  root
RUN apt-get update
RUN apt-get install -y build-essential autoconf autoconf-archive libpqxx-dev libboost-regex-dev libsqlite3-dev
WORKDIR /home/mysql
# RUN git clone https://github.com/JZuming/TxCheck
COPY TxCheck /home/mysql/TxCheck
WORKDIR /home/mysql/TxCheck
RUN autoreconf -if
# RUN ./configure
run ./configure CXXFLAGS=-I/usr/local/mysql/include/
RUN make clean; make -j

USER  root
WORKDIR /home/mysql
ENTRYPOINT bash