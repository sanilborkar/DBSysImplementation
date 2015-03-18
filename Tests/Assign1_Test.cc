#include "../source/DBFile.h"
#include <gtest/gtest.h>
#include "../source/test.h"
#include <stdlib.h>

namespace {

char *dbfile_dir = "../source/DBFiles/lineitem.bin"; // dir where binary heap files should be stored          // SANIL
char *tpch_dir ="/cise/tmp/dbi_sp11/DATA/10M/"; // dir where dbgen tpch files (extension *.tbl) can be found
char *catalog_path = "catalog"; // dir where tpch file is stored                         // SANIL
//relation *rel;

// The fixture for testing class Foo.
class Assign1_Test : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  Assign1_Test() {
    // You can do set-up work for each test here.
    /*
    cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
    cout << " catalog location: \t" << catalog_path << endl;
    cout << " tpch files dir: \t" << tpch_dir << endl;
    cout << " heap files dir: \t" << dbfile_dir << endl;
    cout << " \n\n";

    s = new relation (supplier, new Schema (catalog_path, supplier), dbfile_dir);
    ps = new relation (partsupp, new Schema (catalog_path, partsupp), dbfile_dir);
    p = new relation (part, new Schema (catalog_path, part), dbfile_dir);
    n = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
    li = new relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
    r = new relation (region, new Schema (catalog_path, region), dbfile_dir);
    o = new relation (orders, new Schema (catalog_path, orders), dbfile_dir);
    c = new relation (customer, new Schema (catalog_path, customer), dbfile_dir);
    */
  }

  virtual ~Assign1_Test() {
    // You can do clean-up work that doesn't throw exceptions here.
    /*
    delete s;
    delete p;
    delete ps;
    delete n;
    delete li;
    delete r;
    delete o;
    delete c;
    */
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};

TEST_F(Assign1_Test, Create) {
  //char *dbfile_dir = "DBFiles/lineitem.bin"; // dir where binary heap files should be stored          // SANIL
  DBFile DBFileObj;
  EXPECT_EQ(1, DBFileObj.Create(dbfile_dir, heap, NULL));

}

TEST_F(Assign1_Test, Open) {
  //char *dbfile_dir = "DBFiles/lineitem.bin"; // dir where binary heap files should be stored          // SANIL
  DBFile DBFileObj;
  EXPECT_EQ(1, DBFileObj.Open(dbfile_dir));
  EXPECT_LE(1, DBFileObj.Open(dbfile_dir));
  //EXPECT_GE(0, DBFileObj.Open(dbfile_dir));
}

/*
TEST_F(Assign1_Test, Load) {
  char *dbfile_dir = "DBFiles/lineitem.bin"; // dir where binary heap files should be stored          // SANIL
  char *tpch_dir = "../Dataset/lineitem.tbl"; // dir where tpch file is stored                         // SANIL
  DBFile DBFileObj;
  EXPECT_EQ(1, DBFileObj.Create(dbfile_dir, heap, NULL));
  Schema mySch ("catalog", "lineitem");
  EXPECT_EQ(1, DBFileObj.Load(mySch, tpch_dir));
}
*/

TEST_F(Assign1_Test, LoadFromTPCH) {
  DBFile dbfile;

  dbfile.Create (dbfile_dir, heap, NULL);
  Schema mySch (catalog_path, "lineitem");
  dbfile.Load (mySch, tpch_dir);

  // Load from a non-existent schema
  //mySch = Schema(catalog_path, "xyz");
  //dbfile.Load (mySch, tpch_dir);

  // Load from an invalid tpch dir
  EXPECT_EXIT(dbfile.Load (mySch, "xyz"), ::testing::ExitedWithCode(EXIT_FAILURE), "ERROR*");

  dbfile.Close ();
}

TEST_F(Assign1_Test, GetNext) {
  
  DBFile dbfile;
  dbfile.Open (dbfile_dir);
  dbfile.MoveFirst ();

  Record temp;

  int counter = 0;
  Schema mySch (catalog_path, "lineitem");
  EXPECT_GE(0, dbfile.GetNext (temp));
  dbfile.Close ();
}

TEST_F(Assign1_Test, GetNextPredicate) {
  
  DBFile dbfile;
  dbfile.Open (dbfile_dir);
  dbfile.MoveFirst ();

  Record temp;

  int counter = 0;
  Schema mySch (catalog_path, "lineitem");
  EXPECT_GE(0, dbfile.GetNext (temp));
  dbfile.Close ();
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
  cout << " catalog location: \t" << catalog_path << endl;
  cout << " tpch files dir: \t" << tpch_dir << endl;
  cout << " heap files dir: \t" << dbfile_dir << endl;
  cout << " \n\n";

  return RUN_ALL_TESTS();
}