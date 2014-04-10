all: n2kafka

OBJECTS = engine.o n2kafka.o kafka.o config.o
#CFLAGS = -W -Wall -g -O0 -I/opt/rb/include -L/opt/rb/lib
CFLAGS = -W -Wall -g -O2 -DNDEBUG -I/opt/rb/include -L/opt/rb/lib

n2kafka: $(OBJECTS)
	gcc $(CFLAGS) -o n2kafka $(OBJECTS) -lpthread -ljansson -lrdkafka

clean:
	rm -f $(OBJECTS)

%.o:%.c %.h
	gcc $(CFLAGS) -o $@ $< -c

%.o:%.c
	gcc $(CFLAGS) -o $@ $< -c
