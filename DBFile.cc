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

char *dbFile_dir = "DBFiles/"; // dir where binary heap files should be stored					// SANIL
char *dbFilePath = "DBFiles/lineitem.bin"; // dir where binary heap files should be stored					// SANIL

// SANIL - Initialize the data types
DBFile::DBFile () {
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	// Create the File
	fileObj.Open (READ_FILE, f_path);
	fileType = f_type;	//store the type of the DBFile
	//Saili
	currentPage = 0;	//set the currentPage to zero
	return 1;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
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
	cout << endl << fileObj.Close() << " pages written to disk" << endl;	

}

int DBFile::Open (char *f_path) {
	fileObj.Open (WRITE_FILE, f_path);		//Saili - open the file at the specified path
	pageFetched = 0; 						//Set pageFetched to zero to indicate that no page is in memory since the file has just been opened
	return 1;
}

void DBFile::MoveFirst () {
	currentPage = 0;				//Saili - set the currentPage to zero to indicate first page should be brought into memory next
}

int DBFile::Close () {
	return fileObj.Close();			//Saili - close the file
}

void DBFile::Add (Record &rec) {
	// Append the record to the current page
	int pageAddRes = pageObj.Append(&rec);

	// If the record was not added, the page is full.
	// So, create a new page and then add the record
	if (pageAddRes == 0) {
		// Saili - If this is the first page in the File then we need to add it to 1st location and keep an empty 0th page
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
}

//Saili
int DBFile::GetNext (Record &fetchme) {
	if((currentPage + 1) >= fileObj.GetLength ()){			// check if we are already past the last page in the file
		return 0;
	}
	if(pageFetched == 0){									// check if a page has been fetched in memory
		pageObj.EmptyItOut();
		fileObj.GetPage(&pageObj,currentPage);				// if not then fetch the current page from the file in memory
		pageFetched = 1;									//set pageFetched to one to indicate that a page is in memory
	}
	if(pageObj.GetFirst(&fetchme)==0){						// check if we have already read the last record in the page
		//cout << "Page has no more records\n";
		currentPage +=1;									//increment the current page
		if((currentPage + 1) >= fileObj.GetLength ()){		//check if there are no more pages in the file
			return 0;
		}
		//cout << "Current Page " << currentPage << endl;
		pageObj.EmptyItOut();
		fileObj.GetPage(&pageObj,currentPage);			//fetch the next page from the file
		pageObj.GetFirst(&fetchme);						//fetch the first record from the fetched page
	}
	return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	if((currentPage + 1) >= fileObj.GetLength ()){			// check if we are already past the last page in the file
		return 0;
	}

	if(pageFetched == 0){									// check if a page has been fetched in memory
		pageObj.EmptyItOut();								
		fileObj.GetPage(&pageObj,currentPage);				// if not then fetch the current page from the file in memory
		pageFetched = 1;									//set pageFetched to one to indicate that a page is in memory
	}
	Record currRec;
	ComparisonEngine compEng;
	while(1){												//loop through all the records in the file until a match is found
		if(pageObj.GetFirst(&fetchme)==0){					// check if we have already read the last record in the page
			//cout << "Page has no more records\n";
			currentPage +=1;								//increment the current page
			if((currentPage + 1) >= fileObj.GetLength ()){	//check if there are no more pages in the file
				return 0;
			}
			//cout << "Current Page " << currentPage << endl;
			pageObj.EmptyItOut();
			fileObj.GetPage(&pageObj,currentPage);			//fetch the next page from the file
			pageObj.GetFirst(&currRec);						//fetch the first record from the fetched page
		}
		if(compEng.Compare (&fetchme, &literal, &cnf)){		//compare the fetched record with the CNF literal
			//cout << "Record matched\n";
			break;
		}
	}
	return 1;
}
