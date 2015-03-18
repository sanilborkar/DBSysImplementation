
#define CATALOG_FILE "catalog"

#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include <string.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

//char *dbFile_dir = "DBFiles/temp.file"; // dir where binary heap files should be stored					// SANIL
char *tpch_dir ="../Dataset/"; // dir where dbgen tpch files (extension *.tbl) can be found
char *catalog_path = "catalog"; // full path of the catalog file


int main () {

/*	SANIL - Assignment 1 Implementation	*/
	/*DBFile dbFileObj;

	// Load Schema
	char *table_name = "lineitem.tbl";
	Schema mySchema (CATALOG_FILE, table_name);

	// Bulk Load the DBFile from records present in table_name
	char *fileToLoad = tpch_dir;
	strcat(fileToLoad, table_name);
	dbFileObj.Load(mySchema, fileToLoad);
	
	// Close the DBFile object
	int close = dbFileObj.Close();*/


	//SANIL - Assignment 1 Implementation END


//SANIL - Commenting code to try out Assignment 1
	// try to parse the CNF
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	Schema lineitem ("catalog", "lineitem");

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();

	// now open up the text file and start procesing it
        FILE *tableFile = fopen ("../Dataset/lineitem.tbl", "r");

        Record temp;
        Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		
			if (counter % 10000 == 0) {
				cerr << counter << "\n";
			}

			if (comp.Compare (&temp, &literal, &myComparison)){
				counter++;
				//temp.Print (&mySchema);
			}             	
        }
        cout << "No. of records " << counter << "\n";
}


