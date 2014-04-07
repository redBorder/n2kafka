all: n2kafka

OBJECTS = parse.o engine.o n2kafka.o
CFLAGS = -W -Wall -g -O2 -I/opt/rb/include -L/opt/rb/lib

n2kafka: $(OBJECTS)
	gcc $(CFLAGS) -o n2kafka $(OBJECTS) -lpthread -ljansson

clean:
	rm -f $(OBJECTS)

%.o:%.c %.h
	gcc $(CFLAGS) -o $@ $< -c

%.o:%.c
	gcc $(CFLAGS) -o $@ $< -c