.SUFFIXES: .cc .o

CC=g++

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.cc)
# OBJS:=$(SRCS:.cc=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.cc
TARGET_OBJ:=$(SRCDIR)main.o

# Include more files if you write another source file.
SRCS_FOR_LIB:=$(SRCDIR)file_manager.cc $(SRCDIR)buffer_manager.cc $(SRCDIR)bpt.cc $(SRCDIR)join.cc $(SRCDIR)lock_manager.cc $(SRCDIR)trx_api.cc $(SRCDIR)log_manager.cc
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.cc=.o)

CFLAGS+= -g -std=c++11 -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET):
	$(foreach file, $(SRCS_FOR_LIB), $(CC) $(CFLAGS) -o $(file:.cc=.o) -c $(file) -lpthread;)
	$(CC) $(CFLAGS) -o $(SRCDIR)main.o -c $(SRCDIR)main.cc -lpthread	
	make static_library
	$(CC) $(CFLAGS) -o $@ $(SRCDIR)main.o -L $(LIBS) -lbpt -lpthread

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

library:
	g++ -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)
