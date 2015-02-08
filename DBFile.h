#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

class DBFile {
private:
	// SANIL - Add File object to be stored in DBFile instance
	Page pageObj;
	File fileObj;
	int fileType;
	//Saili - currentPage will store the page number of the page from file currently in memory
	int currentPage;
	int pageFetched;	//pageFetched is a boolean which indicates if a page is fetched in memory or not

public:
	DBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	// SANIL - Push Page to disk
	//int PushPage();

};
#endif
