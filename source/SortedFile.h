#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "DBFile.h"

struct sortInfo
{
	OrderMaker* myOrder;
	int runLength;
};

// Sorted File Implementation
class SortedFile : public GenericDBFile
{
private:
	struct sortInfo* sInfo;
	char *dbfile_loc;
	//BigQ bigQFile;

public:
	SortedFile();
	SortedFile(struct sortInfo* sInfo);
	~SortedFile();
	
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close();
	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	void SwapMode(bool new_mode);
	//void MergeBigQDBFile();
};



#endif
