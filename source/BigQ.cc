#include "BigQ.h"
#include "Record.h"
#include "DBFile.h"
#include "Defs.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Pipe.h"

#include <fstream>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <queue>
#include <vector>
#include <algorithm>

#define PS 10
#define READ_FILE  0
#define WRITE_FILE 1

//#define SCHEMA "part"

using namespace std;

char *fpath = "../source/DBFiles/sortedRunFile";

struct threadParams
{
	OrderMaker* sortorder;
	Pipe* in;
	Pipe* out;
	int numRecs;
};


struct pqRecord
{
	Record *rec;
	int runNo;
	int currentPage;			// Current page in a sorted run
	OrderMaker *sortorder;
	int currRecNum;				// Current Record Number: [0,PAGE_SIZE)
};


struct IsFirstRun {

	bool operator()(const pqRecord& lhs, const pqRecord& rhs)
	{
		ComparisonEngine *cEng = new ComparisonEngine();
		//OrderMaker sortorder  = ::tp->sortorder;
		//cout << endl << " ------------ Comparing ------------------ ";
		//lhs.rec->Print(new Schema ("../source/catalog", SCHEMA));
		//rhs.rec->Print(new Schema ("../source/catalog", SCHEMA));
		//cout << endl;

		//return (lhs.runNo < rhs.runNo);
		if (cEng->Compare(lhs.rec, rhs.rec, lhs.sortorder) < 0)
			return true;
		else
			return false;
}	
};

Record **recArray;
int counter = 0, runNo, runLength;
long int recsRead = 0;
File fileObj;
Page pageObj;


/* TPMMS - PHASE 2 */
void MergeRuns(struct threadParams *tp) {

	Record *temp;
	struct pqRecord pqr;
	// Open the sorted run file for reading
	fileObj.Open (WRITE_FILE, fpath);

	// Vector for k-way comparisons
	std::vector<pqRecord> vRecords;
	std::vector<pqRecord>::iterator it;
	
	cout << "Total runs: " << runNo << endl;
	//Initialize the priority queue
	for(int i=0;i<runNo;i++){
		pageObj.EmptyItOut();
		fileObj.GetPage(&pageObj,i*runLength);

		temp = new Record();
		pageObj.GetFirst(temp);

		pqr.rec = temp;
		pqr.runNo = i;
		pqr.currentPage = 0;
		pqr.currRecNum = 0;
		pqr.sortorder = tp->sortorder;
		vRecords.push_back(pqr);
	}
	//cout << endl << " ----------------------- START MERGING ----------------------------- " << endl << endl;
	long int iter = 0;					// Debugging purpose
	while(vRecords.size() > 0) {
		iter++;
		//cout << "--------------------- ITERATION " << iter << " -----------------------" << endl;
		// Get the minimum record
		it = std::min_element(vRecords.begin(), vRecords.end(), IsFirstRun());
		
		/*cout << endl << "Insert into output pipe: " << endl;
		(*it).rec->Print (new Schema ("../source/catalog", SCHEMA));*/

		tp->out->Insert((*it).rec);
		
		// pqr contains the next element to be inserted into the Pipe
		struct pqRecord pqr = (*it);
		
		// Remove this element from the vector
		vRecords.erase(it);

		pageObj.EmptyItOut();
		fileObj.GetPage(&pageObj,pqr.runNo*runLength+pqr.currentPage);

		// Get the correct record from the page
		for (int r = 0; r <= pqr.currRecNum; r++)
		{
			temp = new Record();
			pageObj.GetFirst(temp);
		}

		temp = new Record();
		pqr.currRecNum = pqr.currRecNum + 1;

		if(pageObj.GetFirst(temp)==0) {
		 if (pqr.currentPage < (runLength-1)){
			pqr.currentPage++;
			pageObj.EmptyItOut();
			if((pqr.runNo*runLength+pqr.currentPage) + 1 < fileObj.GetLength()){
				//cout << "-----" << endl;
				fileObj.GetPage(&pageObj,pqr.runNo*runLength+pqr.currentPage);
				pageObj.GetFirst(temp);
				pqr.currRecNum = 0;			// Reset currRecNum for the newly fetched page
			}
			else
				continue;
		 }
		 else if (pqr.currentPage >= (runLength-1)){
		 	//cout << "Continuing here " << pqr.runNo << " " << pqr.currentPage << endl;
		 	continue;
		 }
			
		}
		if (iter == recsRead){
			cout << "Breaking because all records processed" << endl;
			break;
		}
			
		/*cout << endl << "Insert into vector: " << endl;
		temp->Print (new Schema ("../source/catalog", SCHEMA));*/

		pqr.rec = new Record();
		pqr.rec = temp;
		vRecords.push_back(pqr);
	}
	fileObj.Close();
}

/* Transfer the page records to recArray */
void Transfer()
{
	Record* temp = new Record();
	while(pageObj.GetFirst(temp) != 0)
	{	
		recArray[counter++] = temp;
		temp = new Record();
	}
}

void *GenerateRuns(void *arg) {

	struct threadParams *tp = (struct  threadParams*) arg;

	int i, j;
	bool remRes = true;
	runNo = 0;
	int runCount = 0;
	int pageAddRes;

	fileObj.Open (WRITE_FILE, fpath);
	Record *temp = new Record();

	// Read records from the input pipe
	while(tp->in->Remove(temp)) {
		runNo++;
		recArray = new Record*[tp->numRecs];	
		counter = 0;
		runCount = 0;

		while((runCount < runLength) && remRes) {		
			// If the record was not added, the page is full.
			// So, create a new page and then add the record
			if (pageObj.Append(temp) == 0) {
				runCount++;
				Transfer();
				pageObj.EmptyItOut();
				continue;
			}
			temp = new Record();
			// Get new record from the input pipe
			remRes = tp->in->Remove(temp);		
		}
		Transfer();

		// Sort the records to generate a sorted run
		ComparisonEngine *cEng = new ComparisonEngine();
		for(i=1; i<counter; i++) {
			Record * t = recArray[i];
			j = i-1;
			while(j>=0 && cEng->Compare(recArray[j], t, tp->sortorder) > 0) {
				recArray[j+1] = recArray[j];
				j--;
			}
			recArray[j+1] = t;
		}

		// Print out the sorted run
		//cout << endl << "Sorted Run #: " << runNo << endl;
		pageObj.EmptyItOut();
		int pageCount = 1;
		for(i=0; i< counter; i++){
			//cout << " -----------------" << i << "------------------------- ";
			//if (i==632)
			//recArray[i]->Print (new Schema ("../source/catalog", SCHEMA));

			pageAddRes = pageObj.Append(recArray[i]);

			// If the record was not added, the page is full.
			// So, create a new page and then add the record
			if (pageAddRes == 0) {
				// Saili - If this is the first page in the File then we need to add it to 1st location and keep an empty 0th page
				if (fileObj.GetLength() == 0)
					fileObj.AddPage(&pageObj, fileObj.GetLength());			// Add a new page
				else
					fileObj.AddPage(&pageObj, fileObj.GetLength()-1);			// Add a new page
				pageCount++;
				pageObj.EmptyItOut();										// Empty the pageObj's contents
				//rec.Print (new Schema ("catalog", "lineitem"));
				pageAddRes =  pageObj.Append(recArray[i]);							// Append the current record to this new page	
			}
		}

		if(pageAddRes && pageCount <= runLength){
			if (fileObj.GetLength() == 0)
				fileObj.AddPage(&pageObj, fileObj.GetLength());			// Add a new page
			else
    			fileObj.AddPage(&pageObj, fileObj.GetLength() - 1);
    		pageObj.EmptyItOut();										// Empty the pageObj's contents	
		}
		else
			counter --;							//we haven't added one record in this run and will add it to the next run
    	recsRead += counter;

    	/* If the inner while condition evaluated to FALSE after the retrieval of the
		   last record from the input pipe because the page and run were full, insert that record into the next available 
		   run, and also into recArray */
		if(remRes){
			pageObj.Append(temp);
		}		
	}
	cout << endl << fileObj.Close() << " pages written to disk" << endl;
	// Merge the sorted runs using TPMMS
	MergeRuns(tp);
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	// SANIL
	runLength = runlen;
	int numRecs = runlen*PAGE_SIZE;	
	recArray = new Record*[numRecs];
	int i, j;

	fileObj.Open (READ_FILE, fpath);
	fileObj.Close();

	// Set parameters to be passed to the thread
	struct threadParams tp;
	tp.in = &in;
	tp.out = &out;
	tp.sortorder = &sortorder;
	tp.numRecs = numRecs;

	pthread_t thread3;
	pthread_create (&thread3, NULL, GenerateRuns, (void*)(&tp));
	pthread_join (thread3, NULL);
	cout << endl << "Total records read from input Pipe: " << recsRead << endl;
    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}
