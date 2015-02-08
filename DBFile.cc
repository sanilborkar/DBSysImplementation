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
	//Saili
	currentPage = 0;
	
	/*if (Close() < 0) {
		cerr << "Error while closing file" << endl;
		return 0;
	}*/

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
		//exit(0);
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

    // SANIL - Add the last non-full page to disk
    fileObj.AddPage(&pageObj, fileObj.GetLength() - 1);

    cout << endl << "Total records: " << counter << endl;
	// Once all records have been added, write the file to disk
	cout << endl << fileObj.GetLength() << " pages written to file" << endl;	
	//fileObj.Open(WRITE_FILE, dbFilePath);
	cout << endl << fileObj.Close() << " pages written to disk" << endl;	

}

int DBFile::Open (char *f_path) {
	fileObj.Open (WRITE_FILE, f_path);
	off_t length = fileObj.GetLength();
	cout << "length of file: " << length << endl;
	pageFetched = 0;
//	if (fileObj.myFilDes < 0)
//		return 0;

	return 1;
}

void DBFile::MoveFirst () {
	//Record temp;
	//Saili
	currentPage = 0;
	/*if(pageObj.GetFirst(&temp) == 0)
		cerr << "DBFile::MoveFirst - No records on the Page" << endl;*/
}

int DBFile::Close () {
	//file.Open(WRITE_FILE, dbFilePath);
	//cout << endl << numPages << " pages written to disk" << endl;	
	//Saili
	return fileObj.Close();
}

void DBFile::Add (Record &rec) {
	// Append the record to the current page
	int pageAddRes = pageObj.Append(&rec);

	// If the record was not added, the page is full.
	// So, create a new page and then add the record
	if (pageAddRes == 0) {
		//cerr << "Page FULL - Adding Page to File.." << endl;
		if (fileObj.GetLength() == 0)
			fileObj.AddPage(&pageObj, fileObj.GetLength());			// Add a new page
		else
			fileObj.AddPage(&pageObj, fileObj.GetLength()-1);			// Add a new page

		pageObj.EmptyItOut();										// Empty the pageObj's contents
		
		rec.Print (new Schema ("catalog", "lineitem"));
		pageAddRes =  pageObj.Append(&rec);							// Append the current record to this new page

		//Saili
		currentPage += 1; 											// increment the current page pointer
	}
	//else //if (pageAddRes > 1)
	//	cout << "Record Written: " << pageAddRes << endl;
	
}

int DBFile::GetNext (Record &fetchme) {
	if((currentPage + 1) >= fileObj.GetLength ()){
		return 0;
	}
	if(pageFetched == 0){
		pageObj.EmptyItOut();
		fileObj.GetPage(&pageObj,currentPage);
		pageFetched = 1;
	}
	if(pageObj.GetFirst(&fetchme)==0){
		cout << "Page has no more records\n";
		currentPage +=1;
		if((currentPage + 1) >= fileObj.GetLength ()){
			return 0;
		}
		cout << "Current Page " << currentPage << endl;
		pageObj.EmptyItOut();
		fileObj.GetPage(&pageObj,currentPage);
		pageObj.GetFirst(&fetchme);
	}
	return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	if((currentPage + 1) >= fileObj.GetLength ()){
		return 0;
	}

	if(pageFetched == 0){
		pageObj.EmptyItOut();
		fileObj.GetPage(&pageObj,currentPage);
		pageFetched = 1;
	}
	Record currRec;
	ComparisonEngine compEng;
	while(1){
		if(pageObj.GetFirst(&fetchme)==0){
			cout << "Page has no more records\n";
			currentPage +=1;
			if((currentPage + 1) >= fileObj.GetLength ()){
				return 0;
			}
			cout << "Current Page " << currentPage << endl;
			pageObj.EmptyItOut();
			fileObj.GetPage(&pageObj,currentPage);
			pageObj.GetFirst(&currRec);
		}
		if(compEng.Compare (&fetchme, &literal, &cnf)){
			//fetchme = currRec;
			cout << "Record matched\n";
			break;
		}
	}
	return 1;
}
