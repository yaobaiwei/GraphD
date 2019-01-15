CCOMPILE=mpic++
[Choose one below]
[For 64-bit Linux] PLATFORM=Linux-amd64-64
[For 32-bit Linux] PLATFORM=Linux-i386-32
CPPFLAGS= -I$(HADOOP_HOME)/src/c++/libhdfs -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I [Input the path for system code directory]
LIB = -L$(HADOOP_HOME)/c++/$(PLATFORM)/lib
LDFLAGS = -lhdfs -Wno-deprecated -O2

all: run

run: run.cpp
	$(CCOMPILE) -std=c++11 run.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o run

clean:
	-rm run
