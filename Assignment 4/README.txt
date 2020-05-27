Assignment 04

ADVANCED DATABASE ORGANIZATION

Group No - 20.

Team Members:
Siddharth Mishra
Kanyakumari Kashyap
Vedant Suram

**To run the code:- 

1. Navigate to the project folder in terminal.
2. To clean all .o file in case compiled previously - use "Make clean" command.
3. To compile the program use "Make" command
4. Finally to run the program  -:

use "./test_assign4_1" to run all the test cases.



Functions description -:
1. initIndexManager (void *mgmtData)
This function initialize index manager.

2. shutdownIndexManager () -:
This function is used to free resources and close the index manager.

3. createBtree (char *idxId, DataType keyType, int n)
This function is used to create a new tree with the name idxId, data type and the number in each node.

4. openBtree (BTreeHandle **tree, char *idxId)
This function opens an existing tree.

5. closeBtree (BTreeHandle *tree)
This function is used to free resources and close the tree.

6. deleteBtree (char *idxId)
This functions is used to delete the tree from disk.

7. getNumNodes (BTreeHandle *tree, int *result)
This function is used to get number of nodes from a tree.

8. getNumEntries (BTreeHandle *tree, int *result)
This function is used to get number of entries from a tree.

9. getKeyType (BTreeHandle *tree, DataType *result)
This function is used to get Key Type of a tree.

10. findKey (BTreeHandle *tree, Value *key, RID *result)
This function is used to find a key in tree and return the RID result.

11. insertKey (BTreeHandle *tree, Value *key, RID rid)
To insert a key into a tree this function is used.

12. deleteKey (BTreeHandle *tree, Value *key)
To delete a key from a tree this fuction is called.

13. openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
To initialize the scan handle in tree and prepare for scan this function is used.

14. nextEntry (BT_ScanHandle *handle, RID *result)
While scanning if going on to get the next entry this function is used.

15. closeTreeScan (BT_ScanHandle *handle)
To close the scan handle and free resources this function is used.

16. printTree (BTreeHandle *tree)
To print the tree in the format this function is called.

