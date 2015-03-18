# SANIL - Commented statement, and added new statement to suppress warnings during compilation
#CC = g++ -O2 -Wno-deprecated 
CC = g++ -w

SRC_DIR = source/
BIN_DIR = bin/

tag = -i

ifdef linux
tag = -n
endif

all: clean test

test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o Pipe.o BigQ.o test2.o HeapFile.o SortedFile.o
	$(CC) -o $(BIN_DIR)test.out $(BIN_DIR)Record.o $(BIN_DIR)Comparison.o $(BIN_DIR)ComparisonEngine.o $(BIN_DIR)Schema.o $(BIN_DIR)File.o $(BIN_DIR)DBFile.o $(BIN_DIR)y.tab.o $(BIN_DIR)lex.yy.o $(BIN_DIR)Pipe.o $(BIN_DIR)BigQ.o $(BIN_DIR)test2.o $(BIN_DIR)HeapFile.o $(BIN_DIR)SortedFile.o -pthread -lfl -g

#test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o Assign1_Test.o
#	$(CC) -o $(BIN_DIR)test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o Assign1_Test.o -lgtest -pthread -lfl
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o 
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl
	
Assign1_Test.o: Tests/Assign1_Test.cc
	$(CC) -g -c Tests/Assign1_Test.cc

HeapFile.o: $(SRC_DIR)HeapFile.cc
	$(CC) -g -c $(SRC_DIR)HeapFile.cc -o $(BIN_DIR)HeapFile.o

SortedFile.o: $(SRC_DIR)SortedFile.cc
	$(CC) -g -c $(SRC_DIR)SortedFile.cc -o $(BIN_DIR)SortedFile.o

BigQ.o: $(SRC_DIR)BigQ.cc
	$(CC) -g -c $(SRC_DIR)BigQ.cc -o $(BIN_DIR)BigQ.o

Pipe.o: $(SRC_DIR)Pipe.cc
	$(CC) -g -c $(SRC_DIR)Pipe.cc -o $(BIN_DIR)Pipe.o

test2.o: $(SRC_DIR)test2.cc
	$(CC) -g -c $(SRC_DIR)test2.cc -o $(BIN_DIR)test2.o

main.o: $(SRC_DIR)main.cc
	$(CC) -g -c $(SRC_DIR)main.cc
	
Comparison.o: $(SRC_DIR)Comparison.cc
	$(CC) -g -c $(SRC_DIR)Comparison.cc -o $(BIN_DIR)Comparison.o
	
ComparisonEngine.o: $(SRC_DIR)ComparisonEngine.cc
	$(CC) -g -c $(SRC_DIR)ComparisonEngine.cc -o $(BIN_DIR)ComparisonEngine.o
	
DBFile.o: $(SRC_DIR)DBFile.cc
	$(CC) -g -c $(SRC_DIR)DBFile.cc -o $(BIN_DIR)DBFile.o

File.o: $(SRC_DIR)File.cc
	$(CC) -g -c $(SRC_DIR)File.cc -o $(BIN_DIR)File.o

Record.o: $(SRC_DIR)Record.cc
	$(CC) -g -c $(SRC_DIR)Record.cc -o $(BIN_DIR)Record.o

Schema.o: $(SRC_DIR)Schema.cc
	$(CC) -g -c $(SRC_DIR)Schema.cc -o $(BIN_DIR)Schema.o
	
y.tab.o: $(SRC_DIR)Parser.y
	yacc -d $(SRC_DIR)Parser.y
	mv y.tab.* $(SRC_DIR)
	sed $(tag) $(SRC_DIR)y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c $(SRC_DIR)y.tab.c -o $(BIN_DIR)y.tab.o

lex.yy.o: $(SRC_DIR)Lexer.l
	lex  $(SRC_DIR)Lexer.l
	mv lex.* $(SRC_DIR)
	gcc -c $(SRC_DIR)lex.yy.c -o $(BIN_DIR)lex.yy.o

clean: 
	rm -f $(BIN_DIR)*.o
	rm -f $(BIN_DIR)*.out
	rm -f $(SRC_DIR)y.tab.c
	rm -f $(SRC_DIR)lex.yy.c
	rm -f $(SRC_DIR)y.tab.h
