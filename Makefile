CC = gcc # Flag for implicit rules
CFLAGS = -Wall # Flag for implicit rules. Turn on debug info

# Implicit rule #1: blah is built via the C linker implicit rule
# Implicit rule #2: blah.o is built via the C compilation implicit rule, because blah.c exists
server: server.o


clean:
	rm -f server.o server
