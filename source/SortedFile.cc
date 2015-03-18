#define READ_FILE  0
#define WRITE_FILE 1
#define SORT_FILE ".out"
#define META_FILE "_meta"
#define HEAP 0
#define SORTED 1 
#define BTREE 2

//#define SCHEMA "part"			// Debugging Purpose

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "SortedFile.h"

// SANIL
#include <fstream>	
#include <iostream>
#include <cstring>
#include <stdlib.h>

using namespace std;

char *catalog = "../source/catalog";

bool bWriteSkip;

typedef struct {
	Pipe *pipe;
	struct sortInfo *sInfo;
	char *dbfile_loc;
}testutil;

/* 
	SANIL(A2M2) - Sorted File Functionality
*/

// Producer
void *producerSort (void *arg) {

	testutil *t = (testutil *) arg;
	Pipe *myPipe = t->pipe;

	Record temp;
	int counter = 0;
	int currPage;

	DBFile dbfile;
	dbfile.Open (t->dbfile_loc);
	cout << " producer: opened DBFile " << t->dbfile_loc << endl;
	dbfile.MoveFirst ();

	// Move to the first page
	//currPage = 0;
	//fileObj.GetPage(&pageObj,currentPage);

	while (dbfile.GetNext(temp) == 1) {
		counter += 1;
		if (counter%100000 == 0) {
			 cerr << " producer: " << counter << endl;	
		}

		// SANIL
		//cout << endl << " ---- Producer ---- " << endl;
		//temp.Print (new Schema ("../source/catalog", SCHEMA));

		myPipe->Insert (&temp);
	}

	bWriteSkip = true;
	dbfile.Close ();
	myPipe->ShutDown ();

	cout << " producer: inserted " << counter << " recs into the pipe\n";
}

// Consumer
void *consumerSort (void *arg) {
	
	testutil *t = (testutil *) arg;

	ComparisonEngine ceng;

	DBFile dbfile;
	dbfile.Open (t->dbfile_loc);

	int err = 0;
	int i = 0;

	Record rec[2];
	Record *last = NULL, *prev = NULL;

	cout << endl << "Consumer: Consuming records" << endl;

	while (t->pipe->Remove (&rec[i%2])) {
		prev = last;
		last = &rec[i%2];

		if (prev && last) {
			if (ceng.Compare (prev, last, t->sInfo->myOrder) == 1) {
				err++;
			}

			//cout << endl << " ---- Consumer ---- ";
			//prev->Print (new Schema (catalog, SCHEMA));

			dbfile.Add (*prev);

		}

		//last->Print (new Schema (catalog, SCHEMA));
		i++;
	}

	cout << " consumer: removed " << i << " recs from the pipe\n";

	if (last) {
		//cout << endl << " ---- Consumer ---- ";
		//last->Print (new Schema (catalog, SCHEMA));
		dbfile.Add (*last);
	}
	
	cerr << " consumer: recs removed written out as heap DBFile at " << t->dbfile_loc << endl;
	dbfile.Close ();

	cerr << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
	if (err) {
		cerr << " consumer: " <<  err << " recs failed sorted order test \n" << endl;
	}
}

/* SANIL (A2M2) - Check if the argument is an instance of the class Sorted */
void SortedFile::SwapMode(bool new_mode) {

	if (write_mode == new_mode)
		return;

	Record temp;

	// Already in "WRITE" Mode, new_mode = READ
	if (write_mode)  {

		// Write any page present in memory to the disk
		if (!isDirty) {	// && currentPage == 0)
			write_mode = new_mode;
			return;
		}
		else {

			if (fileObj.GetLength() == 0)
				fileObj.AddPage(&pageObj, fileObj.GetLength());			// Add a new page
			else
				fileObj.AddPage(&pageObj, fileObj.GetLength()-1);			// Add a new page
			
			fileObj.Close();
			pageObj.EmptyItOut();										// Empty the pageObj's contents
			isDirty = false;
			
			if (bWriteSkip) {
				bWriteSkip = false;
				return;
			}
		
			// Use BigQ to write the DBFile as a sorted file to the disk
			int buffsz = 100; // pipe cache size
			Pipe input (buffsz);
			Pipe output (buffsz);	

			//cout << endl << "DBFile loc = " << dbfile_loc;	

			// thread to dump data into the input pipe (for BigQ's consumption)
			pthread_t thread1;
			testutil tutil_prod = {&input, NULL, dbfile_loc};
			pthread_create (&thread1, NULL, producerSort, (void *)&tutil_prod);	

			// thread to read sorted data from output pipe (dumped by BigQ)
			pthread_t thread2;
			char *new_out = (char*)malloc(strlen(dbfile_loc) + strlen(SORT_FILE) + 1);
			sprintf(new_out, "%s%s", dbfile_loc, SORT_FILE);	

			DBFile dbfile;
			dbfile.Create (new_out, sorted, sInfo);
			dbfile.Close();	

			testutil tutil = {&output, sInfo, new_out};
			pthread_create (&thread2, NULL, consumerSort, (void *)&tutil);	

			BigQ bq (input, output, *(sInfo->myOrder), sInfo->runLength);	

			pthread_join (thread1, NULL);
			pthread_join (thread2, NULL);

			/*// Rename the final sorted file ____.bin.out to ____.bin, and
			// ____.bin.out.meta to _____.bin.META_FILE
			char *new_fname = (char*) malloc(strlen(dbfile_loc) - 3);	// -4 (for SORT_FILE) + 1 ('\0') = -3
			memcpy(new_fname, dbfile_loc, strlen(dbfile_loc) - 3);	// 4 = SORT_FILE 
			if (rename(dbfile_loc, new_fname))
				cerr << endl << "Error renaming file from " << dbfile_loc << " to " << new_fname;*/
		}

	}
	// Already in "READ" Mode, new_mode = WRITE
	else {

		if (pageObj.GetFirst(&temp) == 0) { //&& currentPage == 0) {
			write_mode = new_mode;
			return;
		}

		if((currentPage + 1) < fileObj.GetLength ()){		//check if there are no more pages in the file
			if (fileObj.GetLength() == 0)
				fileObj.AddPage(&pageObj, fileObj.GetLength());			// Add a new page
			else
				fileObj.AddPage(&pageObj, fileObj.GetLength()-1);			// Add a new page

			pageObj.EmptyItOut();
			fileObj.GetPage(&pageObj,currentPage);			//fetch the next page from the file
		}


	}

	// Update the mode to new_mode
	write_mode = new_mode;

}



SortedFile::SortedFile() {
}

SortedFile::SortedFile(struct sortInfo* sInfo) {
	write_mode = false;
	this->sInfo = sInfo;
}

int SortedFile::Create (char *fpath, fType file_type, void *startup) {

	//cout << endl << "Creating sorted file: " << fpath;
	fileObj.Open (READ_FILE, fpath);
	dbfile_loc = fpath;
	//fileType = f_type;	//store the type of the DBFile
	//Saili
	currentPage = 0;	//set the currentPage to zero
	write_mode = READ_FILE;
	isDirty = false;
	return 1;
}

int SortedFile::Open (char *f_path) {

	write_mode = READ_FILE;

	// Open the already created sorted file
	fileObj.Open (WRITE_FILE, f_path);		//Saili - open the file at the specified path
	
	dbfile_loc = f_path;
	pageFetched = 0; 						//Set pageFetched to zero to indicate that no page is in memory since the file has just been opened
	return 1;
}

// Needs to be implemented. Just copy-pasted the code from Heap
void SortedFile::Load (Schema &f_schema, char *loadpath) {

	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);


	//cout << endl << "DBFile loc = " << dbfile_loc;

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	testutil tutil_prod = {&input, NULL, dbfile_loc};
	pthread_create (&thread1, NULL, producerSort, (void *)&tutil_prod);

	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	testutil tutil = {&output, sInfo, dbfile_loc};
	pthread_create (&thread2, NULL, consumerSort, (void *)&tutil);

	BigQ bq (input, output, *(sInfo->myOrder), sInfo->runLength);

	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
}

/*
void SortedFile::MergeBigQDBFile() {
	// Shut down BigQ's Input Pipe
	inPipe->ShutDown ();

	Record recPipe, recFile;
	Open();					// Open the DBFile
	MoveFirst();

	DBFile dbfile;
	cout << " DBFile will be created at " << dbFilePath << endl;
	dbfile.Create (dbFilePath, sorted, bigQFile.sInfo);

	// 2-WAY MERGE
	while(outPipe.Remove(&recPipe)) {
		
		if (GetNext(recFile) == 1) {
		
			// Compare the records retrieved from the BigQ Output Pipe and "sorted" DBFile
			ComparisonEngine cEng;
			if (cEng.Compare(recPipe, recFile, bigQFile.sInfo.myOrder) < 0) {
				dbfile.Add(recPipe);
			else
				dbfile.Add(recFile);
			}
		}
		// If GetNext returned a 0, it indicates that the "sorted" DBFile file has no more records
		// Copy all the records one-by-one from the output pipe to the FINAL "sorted" file
		dbfile.Add(recPipe);
	}

	// If the BigQ Output Pipe is exhausted, check if the "sorted" DBFile still contains any
	// "sorted" records. If so, copy them to the FINAL "sorted" file
	while(GetNext(recFile) == 1) {
		dbfile.Add(recFile);
	}

	dbfile.Close();
	outPipe->ShutDown();
}
*/

void SortedFile::MoveFirst () {
	SwapMode(READ_FILE);
	currentPage = 0;				//Saili - set the currentPage to zero to indicate first page should be brought into memory next
}


void SortedFile::Add (Record &rec) {
	
	// Change the mode to "writing"
	SwapMode(WRITE_FILE);

	// Same as HeapFile
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
		//rec.Print (new Schema ("catalog", "lineitem"));
		pageAddRes =  pageObj.Append(&rec);							// Append the current record to this new page
		//Saili
		currentPage += 1; 											// increment the current page pointer
	}

	isDirty = true;

}


int SortedFile::GetNext (Record &fetchme) {

	SwapMode(READ_FILE);

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


int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
	SwapMode(READ_FILE);

	if((currentPage + 1) >= fileObj.GetLength ()){			// check if we are already past the last page in the file
		return 0;
	}

	if(pageFetched == 0){									// check if a page has been fetched in memory
		pageObj.EmptyItOut();								
		fileObj.GetPage(&pageObj,currentPage);				// if not then fetch the current page from the file in memory
		pageFetched = 1;									//set pageFetched to one to indicate that a page is in memory
	}
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
			pageObj.GetFirst(&fetchme);						//fetch the first record from the fetched page
		}
		if(compEng.Compare (&fetchme, &literal, &cnf)){		//compare the fetched record with the CNF literal
			//cout << "Record matched\n";
			break;
		}
	}
	return 1;
}


int SortedFile::Close() {
	SwapMode(READ_FILE);
	return fileObj.Close();			//Saili - close the file
}

SortedFile::~SortedFile() {
}