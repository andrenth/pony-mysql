all:
	gcc -g -Wall -c mypony.c -o mypony.o
	ar rv libmypony.a mypony.o
	ponyc -dp .

clean:
	rm -f *.o *.a
