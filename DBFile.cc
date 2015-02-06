#define READ_FILE  0
#define WRITE_FILE 1

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

// SANIL
#include <fstream>	
#include <iostream>
#include <cstring>

using namespace std;

// stub file .. replace it with your own DBFile.cc

char *dbFile_dir = "DBFiles/"; // dir where binary heap files should be stored					// SANIL
char *dbFilePath = "DBFiles/lineitem.bin"; // dir where binary heap files should be stored					// SANIL

// SANIL - Initialize the data types
DBFile::DBFile () {
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	
	// Create the File
	fileObj.Open (READ_FILE, f_path);
	fileType = f_type;
	
	if (Close() < 0) {
		cerr << "Error while closing file" << endl;
		return 0;
	}

	return 1;

}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	
/*
	// If DB File is present
	if (fileObj.myFileDes > 0) {
		cerr << "Error Opening DB File" << endl;
		exit(0);
	}
*/

	// Create a file handle to the tpch file
	FILE *tpch_ptr = fopen(loadpath, "r");

	if (tpch_ptr == NULL)
	{
		cerr << endl << "ERROR: TPCH File cannot be opened for reading." << endl;
		exit(0);
	}
		

	// Start Loading the DB File from the TPCH file
	int counter = 0;
	Record temp;
        while (temp.SuckNextRecord (&f_schema, tpch_ptr) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		// Add the fetched record to the DB File
		Add(temp);
        }

	// Once all records have been added, write the file to disk
	//cout << endl << fileObj.GetLength() << " pages written to file" << endl;	
	//fileObj.Open(WRITE_FILE, dbFilePath);
	//cout << endl << fileObj.Close() << " pages written to disk" << endl;	

}

int DBFile::Open (char *f_path) {
	fileObj.Open (WRITE_FILE, f_path);
//	if (fileObj.myFilDes < 0)
//		return 0;

	return 1;
}

void DBFile::MoveFirst () {
	Record temp;
	if (pageObj.GetFirst(&temp) == 0)
		cerr << "DBFile::MoveFirst - No records on the Page" << endl;
}

int DBFile::Close () {
	//file.Open(WRITE_FILE, dbFilePath);
	

	//cout << endl << numPages << " pages written to disk" << endl;	
	
	return 1;
}

void DBFile::Add (Record &rec) {
	// Append the record to the current page
	int pageAddRes = pageObj.Append(&rec);

	// If the record was not added, the page is full.
	// So, create a new page and then add the record
	if (pageAddRes == 0) {
		//cerr << "Page FULL - Adding Page to File.." << endl;
		fileObj.AddPage(&pageObj, fileObj.GetLength() + 1);			// Add a new page
		pageObj.EmptyItOut();										// Empty the pageObj's contents
		pageAddRes =  pageObj.Append(&rec);							// Append the current record to this new page
	}
	//else //if (pageAddRes > 1)
	//	cout << "Record Written: " << pageAddRes << endl;
	
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
