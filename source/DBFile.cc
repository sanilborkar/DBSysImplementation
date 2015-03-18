#define READ_FILE  0
#define WRITE_FILE 1
#define META_FILE ".meta"
#define HEAP 0
#define SORTED 1 
#define BTREE 2

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "HeapFile.h"
#include "SortedFile.h"

// SANIL
#include <fstream>	
#include <iostream>
#include <cstring>
#include <stdlib.h>


using namespace std;

//char *dbFile_dir = "../source/DBFiles/"; // dir where binary heap files should be stored					// SANIL
char *dbFilePath = "../source/DBFiles/sortedFile.bin"; // dir where binary heap files should be stored					// SANIL


DBFile::DBFile () {
}


int DBFile::Create (char *f_path, fType f_type, void *startup) {
	fileType = f_type;	//store the type of the DBFile
	//Saili
	//currentPage = 0;	//set the currentPage to zero

	// SANIL (A2M2)
	char *meta_file = (char*)malloc(strlen(f_path) + strlen(META_FILE) + 1);
	sprintf(meta_file, "%s%s", f_path, META_FILE);

	FILE * fptr = fopen(meta_file, "w");
	fwrite(&fileType, sizeof(fileType), 1, fptr);

	// Create an instance of either Heap File or Sorted File as per the input parameter passed
	if (f_type == HEAP)
		myInternalVar = new HeapFile();		

	else if (f_type == SORTED) {
		struct sortInfo* sInfo = (struct sortInfo*) startup;
		OrderMaker *om = sInfo->myOrder;

		/*cout << endl << "WRITING to meta_file: " << meta_file;
		cout << endl << "fileType = " << fileType << endl;
		sInfo->myOrder->Print();
		cout << endl << "runLength = " << sInfo->runLength << endl;*/

		fwrite(&sInfo, sizeof(struct sortInfo), 1, fptr);
		
		myInternalVar = new SortedFile(sInfo);
	}

	fclose(fptr);
	return myInternalVar->Create(f_path, f_type, startup);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	if (myInternalVar)
		myInternalVar->Load(f_schema, loadpath);
	else
		cerr << "ERROR: DBFile::Load - Internal Error";
}

int DBFile::Open (char *f_path) {
	char *meta_file = (char*)malloc(strlen(f_path) + strlen(META_FILE) + 1);
	sprintf(meta_file, "%s%s", f_path, META_FILE);

	int f_type;

	FILE *fptr = fopen(meta_file, "r");
	fread(&f_type, sizeof(f_type), 1, fptr);

	if (f_type == HEAP)
		myInternalVar = new HeapFile();	
	else if (f_type == SORTED) {

		struct sortInfo* sInfo = new struct sortInfo;
		//f_read >> sInfo->myOrder;
		//f_read >> sInfo->runLength;

		fread(&sInfo, sizeof(struct sortInfo), 1, fptr);

		/*cout << endl << "READING from meta_file: " << meta_file;
		cout << endl << "fileType = " << f_type << endl;
		sInfo->myOrder->Print();
		cout << endl << "runLength = " << sInfo->runLength;*/

		myInternalVar = new SortedFile(sInfo);
	}

	//f_read.close();
	fclose(fptr);
	return myInternalVar->Open(f_path);
}

void DBFile::MoveFirst () {
	if (myInternalVar)
		myInternalVar->MoveFirst();
	else
		cout << endl << "ERROR: DBFile::MoveFirst - Internal Error";
}

int DBFile::Close () {
	return myInternalVar->Close();			//Saili - close the file
}

void DBFile::Add (Record &rec) {
	if (myInternalVar)
		myInternalVar->Add(rec);
	else
		cout << endl << "ERROR: DBFile::Add - Internal Error";
}

//Saili
int DBFile::GetNext (Record &fetchme) {
	if (myInternalVar)
		myInternalVar->GetNext(fetchme);
	else
		cout << endl << "ERROR: DBFile::GetNext - Internal Error";
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	if (myInternalVar)
		myInternalVar->GetNext(fetchme, cnf, literal);
	else
		cout << endl << "ERROR: DBFile::GetNext - Internal Error";
	}
