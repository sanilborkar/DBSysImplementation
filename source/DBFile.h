#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"

typedef enum {heap, sorted, tree} fType;

// SANIL (A2M2) - Generic DBFile: to be used by DBFile internally
class GenericDBFile
{
protected:
	Page pageObj;
	File fileObj;
	int currentPage;	//Saili - currentPage will store the page number of the page from file currently in memory
	int pageFetched;	//pageFetched is a boolean which indicates if a page is fetched in memory or not
	bool write_mode;
	bool isDirty;

public:
	virtual int Create (char *fpath, fType file_type, void *startup)=0;
	virtual int Open (char *fpath)=0;
	virtual int Close ()=0;

	virtual void Load (Schema &myschema, char *loadpath)=0;
	
	virtual void MoveFirst ()=0;
	virtual void Add (Record &addme)=0;
	virtual int GetNext (Record &fetchme)=0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal)=0;

	virtual void SwapMode(bool new_mode)=0;
};


class DBFile {
private:
	int fileType;
	// SANIL - Assignment 2, Milestone 2 (A2M2)
	GenericDBFile* myInternalVar;

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

};

#endif