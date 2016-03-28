mypony:
	gcc -g -Wall -c mypony.c -o mypony.o
	ar rv libmypony.a mypony.o

build: mypony
	ponyc -p .

debug: mypony
	ponyc -dp .

all: build

clean:
	rm -f *.o *.a
