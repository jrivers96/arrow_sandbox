#SCIDB=/developement/scidbversion/scidbtrunk/stage/install
ifeq ($(SCIDB),) 
  X := $(shell which scidb 2>/dev/null)
  ifneq ($(X),)
    X := $(shell dirname ${X})
    SCIDB := $(shell dirname ${X})
  endif
endif

# A way to set the 3rdparty prefix path that is convenient
# for SciDB developers.
ifeq ($(SCIDB_VER),)
  SCIDB_3RDPARTY = $(SCIDB)
else
  SCIDB_3RDPARTY = /opt/scidb/$(SCIDB_VER)
endif

# A better way to set the 3rdparty prefix path that does
# not assume an absolute path. You can still use the above
# method if you prefer.
ifeq ($(SCIDB_THIRDPARTY_PREFIX),)
  SCIDB_THIRDPARTY_PREFIX := $(SCIDB_3RDPARTY)
endif
ARROW=/development/sandbox/arrow/cpp/debug/debug
ARROW_INCLUDES=/development/sandbox/arrow/cpp/src/
#PLASMA= 
#JEMALLOC=
# Debug:
#CFLAGS=-pedantic -W -Wextra -Wall -Wno-variadic-macros -Wno-strict-aliasing -Wno-long-long -Wno-unused-parameter -fPIC -D_STDC_FORMAT_MACROS -Wno-system-headers -isystem -g -ggdb3  -D_STDC_LIMIT_MACROS
CFLAGS=-W -Wextra -Wall -Wno-unused-parameter -Wno-variadic-macros -Wno-strict-aliasing -Wno-long-long -Wno-unused -fPIC -D_STDC_FORMAT_MACROS -Wno-system-headers -isystem -O3 -g -DNDEBUG -D_STDC_LIMIT_MACROS
INC=-I. -DPROJECT_ROOT="\"$(SCIDB)\"" -I"$(SCIDB_THIRDPARTY_PREFIX)/3rdparty/boost/include/" -I"$(SCIDB)/include"
-I"$(ARROW_INCLUDES)"
LIBS=-L. -L"$(SCIDB_THIRDPARTY_PREFIX)/3rdparty/boost/lib" -L"$(SCIDB)/lib" -L"$(ARROW)/lib" -L"$(ARROW)"
-Wl,-rpath,$(SCIDB)/lib:$(RPATH) -larrow -lplasma -ljemalloc -lm

SRCS=plugin.cpp 
# Compiler settings for SciDB version >= 15.7
ifneq ("$(wildcard /usr/bin/g++-4.9)","")
  CC := "/usr/bin/gcc-4.9"a
  CXX := "/usr/bin/g++-4.9"
  CFLAGS+=-std=c++11 -DCPP11
  SRCS+= arrowsandbox.cpp
else
  ifneq ("$(wildcard /opt/rh/devtoolset-3/root/usr/bin/gcc)","")
   CC := "/opt/rh/devtoolset-3/root/usr/bin/gcc"
   CXX := "/opt/rh/devtoolset-3/root/usr/bin/g++"
   CFLAGS+=-std=c++11 -DCPP11
   SRCS+= arrowsandbox.cpp
  endif
endif

all:
	@if test ! -d "$(SCIDB)"; then echo  "Error. Try:\n\nmake SCIDB=<PATH TO SCIDB INSTALL PATH>"; exit 1; fi
	$(CXX) $(CFLAGS) $(INC) -o libpull.so $(SRCS) $(LIBS)
	@echo "Now copy *.so to your SciDB lib/scidb/plugins directory and run"
	@echo "iquery -aq \"load_library('pull')\" # to load the plugin."
	@echo
	@echo "Re-start SciDB if the plugin was already loaded previously."
	@echo "Remember to copy the plugin to all your nodes in the cluster."
executable:
	$(CXX) $(CFLAGS) $(INC) -o main $(SRCS) $(LIBS)
test:
	@./test.sh
clean:
	rm -f *.so *.o
