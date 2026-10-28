PROJECT=server client
SOURCES=server.cpp client.cpp lib/libsend.cpp lib/librecv.cpp lib/libcommon.cpp
LIBRARY=nope
INCPATHS= -I lib/include
LIBPATHS=.
LDFLAGS=
CFLAGS=-c -Wall -Werror -Wno-error=unused-variable -g
CC=g++

# ------------ MAGIC BEGINS HERE -------------

# Automatic generation of some important lists
OBJECTS=$(SOURCES:.cpp=.o)
INCFLAGS=$(foreach TMP,$(INCPATHS),-I$(TMP))
LIBFLAGS=$(foreach TMP,$(LIBPATHS),-L$(TMP))

# Set up the output file names for the different output types
#BINARY=$(PROJECT)

all: $(SOURCES) server client

#$(BINARY): $(OBJECTS)
server: $(OBJECTS)
	$(CC) $(LIBFLAGS) $(INCFLAGS) server.cpp lib/librecv.o lib/libcommon.o $(LDFLAGS) -o $@

client: $(OBJECTS)
	$(CC) $(LIBFLAGS) $(INCFLAGS) client.cpp lib/libsend.o lib/libcommon.o $(LDFLAGS) -o $@

.cpp.o:
	$(CC) $(INCFLAGS) $(CFLAGS) -fPIC $< -o $@

clean:
	rm -f $(OBJECTS) server client
