-include Makefile.local

bin_PROGRAMS = transfuzz

AM_CPPFLAGS = -Dfuzz_test
DUT = " "

if DUT_TIDB
DUT += tidb.cc
AM_CPPFLAGS += -DHAVE_TIDB
endif

if DUT_MYSQL
DUT += mysql.cc
AM_CPPFLAGS += -DHAVE_MYSQL
endif

if DUT_MARIADB
DUT += mariadb.cc
AM_CPPFLAGS += -DHAVE_MARIADB
endif

transfuzz_SOURCES = relmodel.cc schema.cc $(DUT)	 			\
    random.cc prod.cc expr.cc grammar.cc impedance.cc	\
    transaction_test.cc transfuzz.cc dbms_info.cc \
    general_process.cc instrumentor.cc dependency_analyzer.cc

transfuzz_LDADD = $(LIBPQXX_LIBS) $(MONETDB_MAPI_LIBS) $(BOOST_REGEX_LIB) $(POSTGRESQL_LIBS) $(BOOST_LDFLAGS) $(POSTGRESQL_LDFLAGS)

AM_CPPFLAGS += $(BOOST_CPPFLAGS) $(LIBPQXX_CFLAGS) $(POSTGRESQL_CPPFLAGS) $(MONETDB_MAPI_CFLAGS) -Wall -Wno-sign-compare -Wextra -fPIC
