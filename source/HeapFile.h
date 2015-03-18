#ifndef HEAPFILE_H
#define HEAPFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "DBFile.h"


// SANIL (A2M2) - Generic DBFile: to be used by DBFile internally
// Heap File Implementation
class HeapFile : public GenericDBFile
{
public:
	HeapFile();
	~HeapFile();

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *f_path);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	void SwapMode(bool new_mode);

};

#endif