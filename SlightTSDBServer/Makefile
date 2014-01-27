# -----------------  Slight TS DB make file 
#

# define the C compiler to use
CC = gcc

# define any compile-time flags
CFLAGS = -Wall -g

# define header files dir other than /usr/include
INCLUDES = -Isrc/base64 -I/src/cJSON -Isrc/sha2

# define any libraries to link into executable:
LIBS = -lpthread -lm

# define the C source files
SRCS = src/sha2/sha2.c src/cJSON/cJSON.c src/base64/base64.c  src/STSAccess.c src/STSCore.c src/STSLog.c src/STSManager.c src/STSjsonrpc.c src/SlightTSDB.c

# define the C object files 
OBJS = $(SRCS:.c=.o)

# define the executable file 
MAIN = SlightTSDB

#
.PHONY: depend clean

all:	$(MAIN)
	@echo  Slight Time Series Data Base server has been compiled. Enjoy!

$(MAIN):	$(OBJS) 
		$(CC) $(CFLAGS) $(INCLUDES) -o $(MAIN) $(OBJS) $(LIBS)

# this is a suffix replacement rule for building .o's from .c's
.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<  -o $@

clean:
	$(RM) src/*.o src/sha2/*.o src/cJSON/*.o src/base64/*.o *~ $(MAIN)


