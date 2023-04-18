FROM ubuntu:20.04 
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y apt-utils
RUN apt-get install -y build-essential python3-dev automake git flex bison libglib2.0-dev libpixman-1-dev python3-setuptools
RUN apt-get install -y vim cmake curl wget gdb

# install latest tidb-server
run apt-get install -y mysql-client-8.0
run apt-get install -y libmysqlclient-dev
# from source
workdir /root
# build golang
run wget https://go.dev/dl/go1.19.1.linux-amd64.tar.gz
run rm -rf /usr/local/go && tar -C /usr/local -xzf go1.19.1.linux-amd64.tar.gz
env PATH=$PATH:/usr/local/go/bin
# build tidb
run git clone -b v5.4.0-alpha https://github.com/pingcap/tidb.git
workdir /root/tidb
run make -j

# get TxCheck
USER  root
RUN apt-get update
RUN apt-get install -y build-essential autoconf autoconf-archive libpqxx-dev libboost-regex-dev libsqlite3-dev
RUN apt-get install -y libmysqlclient-dev
WORKDIR /root
# RUN git clone https://github.com/JZuming/TxCheck
COPY TxCheck /root/TxCheck
WORKDIR /root/TxCheck
RUN autoreconf -if
RUN ./configure
RUN make clean; make -j

USER  root
WORKDIR /root
ENTRYPOINT bash