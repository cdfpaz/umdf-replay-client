# Copyright (c) 2012, Clebson Derivan ( cderivan@gmail.com ).
# All rights reserved.

CC = g++44
CCFLAGS = -Wall -O -O3 
VPATH = ../..

INCLUDES = 	-I ../../../libs/boost_1_51_0 \
	   		-I ../../../libs/quickfast_1_4_0/src \
	   		-I ../../../libs/quickfix-1.13.3/include

LIBS =	-L ../../../libs/boost_1_51_0/stage/lib -lboost_thread -lboost_system -lboost_program_options -lboost_date_time -lboost_filesystem \
		-L ../../../libs/quickfast_1_4_0/lib -lQuickFAST \
		-L ../../../libs/quickfix-1.13.3/lib -lquickfix

BIN = umdf-replay-client

OBJS = umdf-replay-client.o ValueToFix.o

.cc.o:
	$(CC) $(CCFLAGS) $(INCLUDES) -c $< -o $@

.cpp.o:
	$(CC) $(CCFLAGS) $(INCLUDES) -c $< -o $@

$(BIN): $(OBJS)
	$(CC) $(OBJS) -o $(BIN) $(LIBS)

$(OBJS): Makefile

all: $(BIN)

clean:
	rm -f $(BIN) *.o 
