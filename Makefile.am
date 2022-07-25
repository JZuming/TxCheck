-include Makefile.local

BUILT_SOURCES = gitrev.h

bin_PROGRAMS = sqlsmith transfuzz

AM_CPPFLAGS = -Dtest1
DUT = postgres.cc

if DUT_MONETDB
DUT += monetdb.cc
endif

if DUT_SQLITE
DUT += sqlite.cc
endif

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

sqlsmith_SOURCES = relmodel.cc schema.cc $(DUT)	 			\
    random.cc prod.cc expr.cc grammar.cc log.cc dump.cc impedance.cc	\
    sqlsmith.cc

sqlsmith_LDADD = $(LIBPQXX_LIBS) $(MONETDB_MAPI_LIBS) $(BOOST_REGEX_LIB) $(POSTGRESQL_LIBS) $(BOOST_LDFLAGS) $(POSTGRESQL_LDFLAGS)

transfuzz_SOURCES = relmodel.cc schema.cc $(DUT)	 			\
    random.cc prod.cc expr.cc grammar.cc log.cc dump.cc impedance.cc	\
    transaction_test.cc cockroachdb.cc transfuzz.cc dbms_info.cc \
    general_process.cc instrumentor.cc dependency_analyzer.cc

transfuzz_LDADD = $(LIBPQXX_LIBS) $(MONETDB_MAPI_LIBS) $(BOOST_REGEX_LIB) $(POSTGRESQL_LIBS) $(BOOST_LDFLAGS) $(POSTGRESQL_LDFLAGS)

AM_LDFLAGS = -fsanitize=address -fno-omit-frame-pointer
AM_CPPFLAGS += $(BOOST_CPPFLAGS) $(LIBPQXX_CFLAGS) $(POSTGRESQL_CPPFLAGS) $(MONETDB_MAPI_CFLAGS) -Wall -Wno-sign-compare -Wextra -fPIC -fsanitize=address

EXTRA_DIST = gitrev.h dump.hh expr.hh grammar.hh log.hh prod.hh		\
    random.hh relmodel.hh schema.hh impedance.hh known.txt known_re.txt log.sql	\
    README.org TODO.org ast.png logo.png dump.xsl util.hh sqlite.hh	tidb.hh \
    dut.hh postgres.hh monetdb.hh log-v1.0-to-v1.2.sql

gitrev.h: $(HEADERS) $(SOURCES)
	-if git describe --dirty --tags --always > /dev/null ; then \
	echo "#define GITREV \"$$(git describe --dirty --tags --always)\"" > $@ ;\
	fi

filterdump:
	psql -c 'copy (select error from known) to stdout' smith|sort -u > known.txt
	psql -c 'copy (select re from known_re) to stdout' smith|sort -u > known_re.txt

.PHONY: filterdump
