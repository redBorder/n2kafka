all: n2kafka

OBJECTS = engine.o n2kafka.o

n2kafka: $(OBJECTS)
	gcc -g -o n2kafka $(OBJECTS) -lpthread

clean:
	rm -f $(OBJECTS)

%.o:%.c %.h
	gcc -g -W -Wall -o $@ $< -c

%.o:%.c
	gcc -g -W -Wall -o $@ $< -c