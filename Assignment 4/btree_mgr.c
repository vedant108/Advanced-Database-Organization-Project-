#include <math.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "btree_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "storage_mgr.h"
#include "tables.h"




const int BT_NUM_PAGES = 100;
const ReplacementStrategy BT_REPLACEMENT_STRATEGY = RS_LRU;
const int MAX_STRING_KEY_LENGTH = 10;
const int BT_RESERVED_PAGES = 1;

typedef struct BT_BtreeInfoNode {
    int *rootNodeIdx;
    int *keyType;
    int *keySize;
    int *maxKeys;
    int *numNodes;
    int *totalKeys;
} BT_BtreeInfoNode;


typedef struct TreeMgmt{
    BM_BufferPool *bufferPool;
    BT_BtreeInfoNode *infoNode;
    BM_PageHandle *infoPage;
} TreeMgmt;

typedef struct BT_BtreeNode {
    int *selfIdx;
    int *parentIdx;
    int *leftSiblingIdx;
    int *rightSiblingIdx;
    bool *isLeaf;
    int *numKeys;
    char *keyValues;
    int *childrenIdx;
} BT_BtreeNode;



BT_BtreeInfoNode *readTreeInfoNode(BM_PageHandle *nodePage) {
    void *infoNodeSize = malloc(sizeof(BT_BtreeInfoNode));
    BT_BtreeInfoNode *infoNode = (BT_BtreeInfoNode*)infoNodeSize;
    char *data = nodePage->data;
    infoNode->rootNodeIdx = (int*)data;
    int *root = infoNode->rootNodeIdx;
    infoNode->keyType =root+ 1;
    int *key_type =  infoNode->keyType;
    infoNode->keySize = key_type+ 1;
    int *infoKeySize = infoNode->keySize;
    infoNode->maxKeys = infoKeySize+ 1;
    int *number_nodes = infoNode->maxKeys;
    infoNode->numNodes = number_nodes + 1;
    int *totalKeys = infoNode->numNodes;
    infoNode->totalKeys = totalKeys + 1;

    return infoNode;
}


BT_BtreeNode *readTreeNodePage(BM_PageHandle *nodePage) {
    void *nodeSize = malloc(sizeof(BT_BtreeNode));
    char * data = nodePage->data;
    BT_BtreeNode *node = (BT_BtreeNode*)nodeSize;
    node->selfIdx = (int*)(data);
    node->parentIdx = node->selfIdx + 1;
    int *left =node->leftSiblingIdx;
    node->leftSiblingIdx = node->parentIdx + 1;
    int *right = node->rightSiblingIdx;
    node->rightSiblingIdx= node->leftSiblingIdx + 1;
    node->numKeys = node->rightSiblingIdx + 1;
    int intsize = sizeof(int);
    int boolsize = sizeof(bool);
    int *num = node->numKeys;
    node->isLeaf = (bool*)((char*)num + intsize);
    node->keyValues = ((char*)node->isLeaf + boolsize);
    int *nodedata = nodePage->data;
    node->childrenIdx = (int*)((char*)nodedata + PAGE_SIZE - intsize);
    return node;
}


void initializeNewNode(BT_BtreeNode *node, int selfIdx) {
    int *n_selfindex = node->selfIdx;
    *(n_selfindex) = selfIdx;
    int *p_index = node->parentIdx;
    *(p_index) = -1;
    int *l_index = node->leftSiblingIdx;
    *(l_index) = -1;
    int *r_index = node->rightSiblingIdx;
    *(r_index) = -1;
    bool *leaf = node->isLeaf;
    *(leaf) = TRUE;
    *(node->numKeys) = 0;
}


Value *readKeyValue(char *keyPtr, DataType keyType) {
    Value *keyValue = NULL;
    if(keyType==DT_INT){
        MAKE_VALUE(keyValue, DT_INT, *(int*)(keyPtr));


    }
    else if(keyType==DT_STRING){
        MAKE_STRING_VALUE(keyValue, keyPtr);


    }
    else if(keyType==DT_FLOAT){
        MAKE_VALUE(keyValue, DT_FLOAT, *(float*)keyPtr);


    }
    else if(keyType==DT_BOOL){
        MAKE_VALUE(keyValue, DT_BOOL, *(bool*)keyPtr);


    }


    return keyValue;
}


void writeKeyValue(char *keyPtr, Value *key, DataType keyType, int keySize) {
    Value *keyValue = NULL;
    if (keyType == DT_INT) {
        memcpy(keyPtr, &(key->v.intV), keySize);
    } else if (keyType == DT_STRING) {
        memcpy(keyPtr, key->v.stringV, keySize);

    } else if (keyType == DT_FLOAT) {
        memcpy(keyPtr, &(key->v.floatV), keySize);

    } else if (keyType == DT_BOOL) {
        memcpy(keyPtr, &(key->v.boolV), keySize);

    }
}



int lowerBoundIdx(BT_BtreeInfoNode *infoNode, BT_BtreeNode *node, Value *key) {

    int keyIdx;
    int *keyTypePointer=infoNode->keyType;
    int *keySize_val = infoNode->keySize;
    DataType keyType = *(keyTypePointer);

    int keySize = *(keySize_val);

    int numKeys = *(node->numKeys);


    char *currKeyValuePtr = node->keyValues;

    Value *currKeyValue;
    Value *comparisionResult = (Value*)malloc(sizeof(Value));

    for(keyIdx=0;keyIdx < numKeys;keyIdx++) {
        currKeyValue = readKeyValue(currKeyValuePtr, keyType);
        valueSmaller(key, currKeyValue, comparisionResult);
        freeVal(currKeyValue);

        if(comparisionResult->v.boolV == TRUE) {
            break;
        }


        currKeyValuePtr += keySize;
    }

    free(comparisionResult);
    return keyIdx;
}






int findLeafNodeForKey(BTreeHandle *tree, Value *key) {
    void *mgmtData = tree->mgmtData;
    void *BMPageHandleSize = malloc(sizeof(BM_PageHandle));
    TreeMgmt *treeMgmt = (TreeMgmt *)(mgmtData);
    BT_BtreeInfoNode *infoNode = treeMgmt->infoNode;

    int nodeIdx = *(infoNode->rootNodeIdx);
    BM_PageHandle *nodePage = (BM_PageHandle*)BMPageHandleSize;
    markDirty(treeMgmt->bufferPool, nodePage);
    pinPage(treeMgmt->bufferPool, nodePage, nodeIdx);

    BT_BtreeNode *node = readTreeNodePage(nodePage);


    while(*(node->isLeaf) != TRUE ) {

        int keyIdx = lowerBoundIdx(infoNode, node, key);
        unpinPage(treeMgmt->bufferPool, nodePage);
        nodeIdx = (node->childrenIdx)[-keyIdx];

        free(node);
        markDirty(treeMgmt->bufferPool, nodePage);
        pinPage(treeMgmt->bufferPool, nodePage, nodeIdx);

        node = readTreeNodePage(nodePage);
    }
    unpinPage(treeMgmt->bufferPool, nodePage);
    free(nodePage);
    free(node);

    return nodeIdx;
}


void splitNode(BT_BtreeInfoNode *infoNode, BT_BtreeNode *leftSibling, BT_BtreeNode *rightSibling, int splitIdx) {
    int *k_size = infoNode->keySize;
    int *keyval_left  = leftSibling->numKeys;
    bool *leaf = leftSibling->isLeaf;

    int keySize = *(k_size);
    int numKeys = *(keyval_left);
    bool isLeaf = *(leaf);


    *(rightSibling->isLeaf) = isLeaf;

    int *rightParentIdx =rightSibling->parentIdx;
    *(rightParentIdx) = *(leftSibling->parentIdx);
    *(rightSibling->rightSiblingIdx) = *(leftSibling->rightSiblingIdx);

    int *left_Sibling = leftSibling->rightSiblingIdx;
    *(left_Sibling) = *(rightSibling->selfIdx);
    int *right = rightSibling->leftSiblingIdx;
    *(right) = *(leftSibling->selfIdx);


    int leftSiblingSize = splitIdx;
    int rightSiblingSize = numKeys - splitIdx;

    *(leftSibling->numKeys) = leftSiblingSize;
    int *rightKeys = rightSibling->numKeys;
    *(rightKeys) = rightSiblingSize;


    memcpy( (rightSibling->keyValues), (leftSibling->keyValues + splitIdx*keySize), rightSiblingSize*keySize);


    if(isLeaf!=FALSE) {
        int size = sizeof(int);
        int rightSib =rightSiblingSize*size;
        int sibsize = -2*rightSiblingSize+1;
        memcpy( &(rightSibling->childrenIdx[sibsize]), &(leftSibling->childrenIdx[-2*numKeys+1]), 2*rightSib);
    }
    else {
        int size = sizeof(int);
        int rightSib =rightSiblingSize*size;
        memcpy( &(rightSibling->childrenIdx[-rightSiblingSize]), &(leftSibling->childrenIdx[-numKeys]), rightSib);
    }
}


void shiftKeysAndPointers(BT_BtreeInfoNode *infoNode, BT_BtreeNode *node, int startIdx, int shiftCount) {
    int *k_size = infoNode->keySize;
    int *n_size = node->numKeys;
    bool *leaf = node->isLeaf;
    int keySize = *(k_size);

    int numKeys = *(n_size);
    bool isLeaf = *(leaf);

    int s_index = numKeys - startIdx;
    memmove( (node->keyValues + (startIdx+shiftCount) * keySize), (node->keyValues + startIdx * keySize), (s_index) * keySize);


    if(isLeaf!=FALSE) {
        int size = sizeof(int);
        int s_index = numKeys - startIdx;
        memmove( (node->childrenIdx - 2*(numKeys+shiftCount) + 1), (node->childrenIdx - 2*numKeys + 1), 2*(s_index)*size );
    }
    else {
        int size = sizeof(int);
        int s_index = numKeys - startIdx;
        memmove( (node->childrenIdx - numKeys - shiftCount), (node->childrenIdx - numKeys), (s_index)*size );
    }
}


void insertKeyAndIndexIntoNode(BTreeHandle *tree, BT_BtreeNode *node, Value *key, void *value) {
    BT_BtreeInfoNode *infoNode;
    TreeMgmt *treeMgmt = (TreeMgmt *)(tree->mgmtData);

    infoNode= treeMgmt->infoNode;
    DataType keyType = *(infoNode->keyType);
    int keyval=*(infoNode->keySize);
    int keySize = keyval;

    int num= *(node->numKeys);
    int max = *(infoNode->maxKeys);
    if(num >= max) {
        int *nodeCount = infoNode->numNodes;
        (*(nodeCount))++;

        void *handle = malloc(sizeof(BM_PageHandle));
        BM_PageHandle *parentPage = (BM_PageHandle*)handle;
        BT_BtreeNode *parentNode;

        int rightSiblingIdx = *(nodeCount) + BT_RESERVED_PAGES - 1;

        void *handle_2 = malloc(sizeof(BM_PageHandle));
        BM_PageHandle *rightSiblingPage = (BM_PageHandle*)handle_2;
        markDirty(treeMgmt->bufferPool, rightSiblingPage);
        pinPage(treeMgmt->bufferPool, rightSiblingPage, rightSiblingIdx);
        BT_BtreeNode *rightTreenode= readTreeNodePage(rightSiblingPage);

        BT_BtreeNode *rightSiblingNode = rightTreenode;
        initializeNewNode(rightSiblingNode, rightSiblingIdx);

        int lowerBoundValue = lowerBoundIdx(infoNode, node, key);


        int keyIdx = lowerBoundValue;

        int *max=infoNode->maxKeys;

        int splitIdx = (*(max))/2;

        if(keyIdx>splitIdx) {
            splitIdx=splitIdx+1;
            splitNode(infoNode, node, rightSiblingNode,splitIdx);
            insertKeyAndIndexIntoNode(tree, rightSiblingNode, key, value);
        }
        else {
            splitNode(infoNode, node, rightSiblingNode, splitIdx);
            insertKeyAndIndexIntoNode (tree, node, key, value);
        }

        Value *keytoParent = readKeyValue(rightSiblingNode->keyValues, keyType);

        Value *keyIntoParent = keytoParent;

        int *parentIndx = node->parentIdx;

        if(*(parentIndx) == -1) {
            int *nodeCount = infoNode->numNodes;
            (*(nodeCount))++;
            int parentIdx = *(nodeCount) + BT_RESERVED_PAGES - 1;
            pinPage(treeMgmt->bufferPool, parentPage, parentIdx);
            markDirty(treeMgmt->bufferPool, parentPage);

            BT_BtreeNode *pnode=readTreeNodePage(parentPage);

            parentNode = pnode;
            initializeNewNode(parentNode, parentIdx);

            bool *leaf = parentNode->isLeaf;
            *(leaf) = FALSE;
            int *numKeys = parentNode->numKeys;
            *(numKeys) = 1;

            int *parentInd =node->parentIdx;
            *(parentInd) = parentIdx;
            *(rightSiblingNode->parentIdx) = parentIdx;

            // update the tree's root
            int *rindex= infoNode->rootNodeIdx;
            *(rindex) = parentIdx;
            int size= sizeof(int);

            memcpy(parentNode->childrenIdx, (node->selfIdx), size);
            memcpy(parentNode->childrenIdx - 1, &rightSiblingIdx, size);

            writeKeyValue(parentNode->keyValues, keyIntoParent, keyType, keySize);
        }

        else {

            pinPage(treeMgmt->bufferPool, parentPage, *(node->parentIdx));
            markDirty(treeMgmt->bufferPool, parentPage);
            BT_BtreeNode *pnode = readTreeNodePage(parentPage);

            parentNode = pnode;

            insertKeyAndIndexIntoNode(tree, parentNode, keyIntoParent, &rightSiblingIdx);
        }

        freeVal(keyIntoParent);

        unpinPage(treeMgmt->bufferPool, rightSiblingPage);
        free(rightSiblingNode);
        free(rightSiblingPage);


        unpinPage(treeMgmt->bufferPool, parentPage);
        free(parentNode);
        free(parentPage);

    }
    else {
        int *keyval = infoNode->keyType;
        int *keysize = infoNode->keySize;
        DataType keyType = *(keyval);
        int keySize = *(keysize);
        bool isLeaf = *(node->isLeaf);
        int lowerBoundValue = lowerBoundIdx(infoNode, node, key);
        int keyIdx = lowerBoundValue;
        shiftKeysAndPointers(infoNode, node, keyIdx, 1);
        writeKeyValue(node->keyValues + keyIdx*keySize, key, keyType, keySize);

        if(isLeaf!=FALSE) {
            int size = sizeof(int);

            memcpy(node->childrenIdx - 2*keyIdx - 1, value, 2*size);
        }
        else {
            int size = sizeof(int);

            memcpy(node->childrenIdx - keyIdx - 1, value, size);
        }

        int *keyCounter = node->numKeys;
        (*(keyCounter))++;
    }
}



RC createBtree (char *idxId, DataType keyType, int n) {
    void *poolSize = malloc(sizeof(BM_BufferPool));
    void *handleSize = malloc(sizeof(BM_PageHandle));
    RC rc;
    rc = createPageFile(idxId);
    if (rc != RC_OK)
        return rc;

    BM_BufferPool *bufferPool = (BM_BufferPool*)poolSize;

    BM_PageHandle *infoPage = (BM_PageHandle*)handleSize;
    initBufferPool(bufferPool, idxId, BT_NUM_PAGES, BT_REPLACEMENT_STRATEGY, NULL);
    pinPage(bufferPool, infoPage, 0);
    markDirty(bufferPool, infoPage);

    int keySize;
    switch(keyType){
        case DT_INT:
            keySize = sizeof(int);
            break;
        case DT_FLOAT:
            keySize = sizeof(float);
            break;
        case DT_BOOL:
            keySize = sizeof(bool);
            break;
        case DT_STRING:
            keySize = MAX_STRING_KEY_LENGTH * sizeof(char);
            break;
        }

    BT_BtreeInfoNode *infoNode;

    infoNode= readTreeInfoNode(infoPage);

    *(infoNode->rootNodeIdx) = BT_RESERVED_PAGES;
    int *key_type = infoNode->keyType;                             // root node index
    *(key_type) = keyType;
    int *key_node = infoNode->keySize;
    *(key_node) = keySize;
    int *maxKeys = infoNode->maxKeys; // size of given type
    *(maxKeys) = n;
    int *numnodes=infoNode->numNodes;
    *(numnodes) = 1;                          // total number of nodes; by default, only root node
    *(infoNode->totalKeys) = 0;                         // total number of keys
    void *handle = malloc(sizeof(BM_PageHandle));
    unpinPage(bufferPool, infoPage);
    free(infoPage);
    free(infoNode);

    BM_PageHandle *treeRootNodePage = (BM_PageHandle*)handle;
    pinPage(bufferPool, treeRootNodePage, BT_RESERVED_PAGES);
    markDirty(bufferPool, treeRootNodePage);

    BT_BtreeNode *treeRoot = readTreeNodePage(treeRootNodePage);

    BT_BtreeNode *treeRootNode = treeRoot;

    // initialize the root
    initializeNewNode(treeRootNode, BT_RESERVED_PAGES);

    unpinPage(bufferPool, treeRootNodePage);
    free(treeRootNode);
    free(treeRootNodePage);


    shutdownBufferPool(bufferPool);
    free(bufferPool);
    return RC_OK;
}



RC closeBtree (BTreeHandle *tree) {
    tree->idxId = NULL;
    void *x = tree->mgmtData;
    TreeMgmt *treeMgmt = (TreeMgmt *)(x);
    unpinPage(treeMgmt->bufferPool, treeMgmt->infoPage);
    shutdownBufferPool(treeMgmt->bufferPool);
    free(treeMgmt->infoPage);
    free(treeMgmt->infoNode);
    free(treeMgmt);
    free(tree);
    return RC_OK;
}



RC getNumNodes (BTreeHandle *tree, int *result) {
    BT_BtreeInfoNode *infoNode;
    infoNode= ((TreeMgmt*)(tree->mgmtData))->infoNode;
    int *totalNodes = *(infoNode->numNodes);
    *result = totalNodes;
    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result) {
    BT_BtreeInfoNode *infoNode;
    infoNode = ((TreeMgmt*)(tree->mgmtData))->infoNode;
    int *totalkeys = *(infoNode->totalKeys);
    *result = totalkeys;
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result) {
    DataType j = tree->keyType;
    *result = j;
    return RC_OK;
}

RC initIndexManager (void *mgmtData) {
    initStorageManager();
    return RC_OK;
}

RC shutdownIndexManager () {
    return RC_OK;
}


RC findKey (BTreeHandle *tree, Value *key, RID *result) {

    BT_BtreeInfoNode *infoNode;
    void *pageHandleSize= malloc(sizeof(BM_PageHandle));
    TreeMgmt *treeMgmt = (TreeMgmt *)(tree->mgmtData);
    infoNode= treeMgmt->infoNode;
    int *k_type =infoNode->keyType;
    int *k = infoNode->keySize;
    DataType keyType = *(k_type);
    int keySize = *(k);


    int nodeIdx = findLeafNodeForKey(tree, key);

    BM_PageHandle *nodePage = (BM_PageHandle*)pageHandleSize;
    markDirty(treeMgmt->bufferPool, nodePage);
    pinPage(treeMgmt->bufferPool, nodePage, nodeIdx);


    BT_BtreeNode *node = readTreeNodePage(nodePage);
    int lower_index = lowerBoundIdx(infoNode, node, key) - 1;


    int keyIdx = lower_index;

    Value *keyValue = readKeyValue((node->keyValues + keyIdx*keySize), keyType);

    void *value = malloc(sizeof(Value));

    Value *comparisionResult = (Value*)value;

    valueEquals(key, keyValue, comparisionResult);
    freeVal(keyValue);

    RC rc = RC_IM_KEY_NOT_FOUND;

    if(comparisionResult->v.boolV != FALSE) {
        int intsize = sizeof(int);
        rc = RC_OK;
        memcpy(result, (node->childrenIdx - 2*keyIdx - 1), 2*intsize);
    }

    unpinPage(treeMgmt->bufferPool, nodePage);
    free(nodePage);
    free(comparisionResult);

    free(node);

    return rc;
}

RC openBtree (BTreeHandle **tree, char *idxId) {
    void *poolSize = malloc(sizeof(BM_BufferPool));
    void *pghandl = malloc(sizeof(BM_PageHandle));
    BM_BufferPool *bufferPool = (BM_BufferPool*)poolSize;
    initBufferPool(bufferPool, idxId, BT_NUM_PAGES, BT_REPLACEMENT_STRATEGY, NULL);  // initialize a new buffer pool

    BM_PageHandle *infoPage = (BM_PageHandle*)pghandl;
    markDirty(bufferPool, infoPage);
    pinPage(bufferPool, infoPage, 0);


    BT_BtreeInfoNode *infoNode = readTreeInfoNode(infoPage);
    void *treeHandle = malloc(sizeof(BTreeHandle));
    *tree = (BTreeHandle *)treeHandle;
    (*tree)->idxId = idxId;
    (*tree)->keyType = *(infoNode->keyType);
    void *Tree = malloc(sizeof(TreeMgmt));
    TreeMgmt *treeMgmt = (TreeMgmt *) Tree ;
    BM_BufferPool *pool = treeMgmt->bufferPool;
    treeMgmt->bufferPool = bufferPool;
    treeMgmt->infoNode = infoNode;
    treeMgmt->infoPage = infoPage;
    (*tree)->mgmtData = treeMgmt;

    return RC_OK;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid) {

    void *pageHandleSize= malloc(sizeof(BM_PageHandle));
    BT_BtreeInfoNode *infoNode;
    TreeMgmt *treeMgmt = (TreeMgmt *)(tree->mgmtData);

    infoNode= treeMgmt->infoNode;


    int nodeIdx = findLeafNodeForKey(tree, key);

    BM_PageHandle *nodePage = (BM_PageHandle*)pageHandleSize;
    markDirty(treeMgmt->bufferPool, nodePage);
    pinPage(treeMgmt->bufferPool, nodePage, nodeIdx);


    BT_BtreeNode *node = readTreeNodePage(nodePage);
    int *keytotal = infoNode->totalKeys;
    insertKeyAndIndexIntoNode(tree, node, key, &rid);


    (*(keytotal))++;

    unpinPage(treeMgmt->bufferPool, nodePage);
    free(nodePage);
    free(node);

    return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key) {

    void *pageHandleSize= malloc(sizeof(BM_PageHandle));
    TreeMgmt *treeMgmt = (TreeMgmt *)(tree->mgmtData);
    BT_BtreeInfoNode *infoNode;

    int *key_Node=infoNode->keySize;
    int *key_type = infoNode->keyType;
    infoNode= treeMgmt->infoNode;
    DataType keyType = *(key_type);
    int keySize = *(key_Node);


    int nodeIdx = findLeafNodeForKey(tree, key);


    BM_PageHandle *nodePage = (BM_PageHandle*)pageHandleSize;
    markDirty(treeMgmt->bufferPool, nodePage);
    pinPage(treeMgmt->bufferPool, nodePage, nodeIdx);
    BT_BtreeNode *ref=readTreeNodePage(nodePage);


    BT_BtreeNode *node =ref ;


    int index_val = lowerBoundIdx(infoNode, node, key);
    int keyIdx = index_val  - 1;

    Value *keyValue = readKeyValue((node->keyValues + keyIdx*keySize), keyType);

    void *value = malloc(sizeof(Value));
    Value *comparisionResult = (Value*)value;

    valueEquals(key, keyValue, comparisionResult);
    freeVal(keyValue);

    RC rc = RC_IM_KEY_NOT_FOUND;


    if(comparisionResult->v.boolV != FALSE) {
        rc = RC_OK;
        int *keytotal = infoNode->totalKeys;
        int *keyCount = node->numKeys;
        shiftKeysAndPointers(infoNode, node, keyIdx+1, -1);
        (*(keytotal))--;
        (*(keyCount))--;
    }


    unpinPage(treeMgmt->bufferPool, nodePage);
    free(comparisionResult);
    free(nodePage);
    free(node);

    return rc;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) {
    void *pageHandleSize= malloc(sizeof(BM_PageHandle));
    TreeMgmt *treeMgmt = (TreeMgmt *)(tree->mgmtData);
    BT_BtreeInfoNode *infoNode;
    infoNode = treeMgmt->infoNode;
    int *rootIndex=infoNode->rootNodeIdx;
    int nodeIdx = *(rootIndex);


    BM_PageHandle *nodePage = (BM_PageHandle*)pageHandleSize;
    markDirty(treeMgmt->bufferPool, nodePage);
    pinPage(treeMgmt->bufferPool, nodePage, nodeIdx);


    BT_BtreeNode *node = readTreeNodePage(nodePage);

    while( *(node->isLeaf) != TRUE ) {


        unpinPage(treeMgmt->bufferPool, nodePage);
        nodeIdx = (node->childrenIdx)[0];

        free(node);

        markDirty(treeMgmt->bufferPool, nodePage);
        pinPage(treeMgmt->bufferPool, nodePage, nodeIdx);

        node = readTreeNodePage(nodePage);
    }

    unpinPage(treeMgmt->bufferPool, nodePage);
    free(nodePage);
    free(node);
    void *size_RID = malloc(sizeof(RID));
    RID *id = (RID*)size_RID;


    id->page = nodeIdx;
    id->slot = 0;

    void *bhandleSize = malloc(sizeof(BT_ScanHandle));

    *handle = (BT_ScanHandle*)bhandleSize;
    (*handle)->tree = tree;
   (*handle)->mgmtData=NULL;
    (*handle)->mgmtData=id;




    return RC_OK;
}

RC deleteBtree (char *idxId) {
    RC rc = destroyPageFile(idxId);
    if(rc == RC_OK) {
        return RC_OK;

    }
    return rc;
}

RC nextEntry (BT_ScanHandle *handle, RID *result) {
    BTreeHandle *tree;
    RID *id;
    tree = handle->tree;
    TreeMgmt *treeMgmt = (TreeMgmt *)(tree->mgmtData);

    id = (handle->mgmtData);

    int nodeIdx = id->page;
    int keyIdx = id->slot;

    void *pageHandleSize = malloc(sizeof(BM_PageHandle));

    BM_PageHandle *nodePage = (BM_PageHandle*)pageHandleSize;
    pinPage(treeMgmt->bufferPool, nodePage, nodeIdx);

    BT_BtreeNode *node = readTreeNodePage(nodePage);

    int *indexValue = node->numKeys;
    if(*(indexValue) == keyIdx) {
        int *rightSiblingIndex = node->rightSiblingIdx;
        id->page = *(rightSiblingIndex);
        id->slot = 0;

        unpinPage(treeMgmt->bufferPool, nodePage);
        free(node);

        // if no more right siblings
        int x = id->page;


        if (x == -1) {
            return RC_IM_NO_MORE_ENTRIES;
        } else {
            pinPage(treeMgmt->bufferPool, nodePage, x);
            node = readTreeNodePage(nodePage);
        }
    }


    do {
        int RIDSize = sizeof(RID);
        memcpy(result, (node->childrenIdx - 2*(id->slot) - 1), RIDSize);

    } while(result->page == 0 || result->slot == 0);

    id->slot++;
    free(node);
    return RC_OK;
}



char *dfs(BTreeHandle *tree, BT_BtreeNode *curNode, int *idx) {
    void *mgmtData = tree->mgmtData;
    BT_BtreeInfoNode *infoNode=NULL;
    void *pageHandleSize = malloc(sizeof(BM_PageHandle));
    VarString *result;
    VarString *childStr;
    int *key_s=infoNode->keyType;
    int *node_s=infoNode->keySize;
    int *num_keys = curNode->numKeys;
    bool *leaf = curNode->isLeaf;

    MAKE_VARSTRING(result);
    MAKE_VARSTRING(childStr);

    TreeMgmt *treeMgmt = (TreeMgmt *)(mgmtData);
    infoNode= treeMgmt->infoNode;

    BM_PageHandle *curChildPage = (BM_PageHandle*)pageHandleSize;
    BT_BtreeNode *curChildNode;

    Value *keyValue;
    char *serializedKeyValue;

    DataType keyType = *(key_s);
    int keySize = *(node_s);
    int numKeys = *(num_keys);
    bool isLeaf = *(leaf);

    APPEND(result, "(%d", *idx);
    int j = (*idx);
    j++;

    for(int i = 0; i<=numKeys; i++) {
        if(isLeaf!=NULL) {
            int curChildPageIdx;
            memcpy(&curChildPageIdx, (curNode->childrenIdx - i), sizeof(int));
            if(curChildPageIdx == 0)
                continue;

            curChildNode = readTreeNodePage(curChildPage);
            pinPage(treeMgmt->bufferPool, curChildPage, curChildPageIdx);


            unpinPage(treeMgmt->bufferPool, curChildPage);
            char *curChildStr = dfs(tree, curChildNode, idx);
            APPEND(result, "%d,", *idx);
            APPEND_STRING(childStr, curChildStr);

            free(curChildNode);
        }
        else if(i<numKeys) {
            void *RID_size = malloc(sizeof(RID));
            RID *id = (RID*)RID_size;
            APPEND(result, "%d.%d,", id->page, id->slot);
            memcpy(id, (curNode->childrenIdx - 2*i - 1), 2*sizeof(int));

            free(id);
        }
        if(i<numKeys) {
            char *serializedKeyValue;
            keyValue = readKeyValue((curNode->keyValues + i*keySize), keyType);
            serializedKeyValue = serializeValue(keyValue);
            APPEND(result, "%s,", serializedKeyValue);
            free(serializedKeyValue);
        }
    }
    char *childStrBuf;
    GET_STRING(childStrBuf, childStr);
    APPEND_STRING(result, "]\n");
    APPEND_STRING(result, childStrBuf);

    RETURN_STRING(result);
}

char *printTree (BTreeHandle *tree) {
    char *res = NULL;
    void * mgmtData = tree->mgmtData;
    TreeMgmt *treeMgmt = (TreeMgmt *)mgmtData;
    BT_BtreeInfoNode *infoNode = treeMgmt->infoNode;

    int rootNodeIdx = *(infoNode->rootNodeIdx);
    void *BMPageHandleSize = malloc(sizeof(BM_PageHandle));

    BM_PageHandle *rootNodePage = (BM_PageHandle*)BMPageHandleSize;

    pinPage(treeMgmt->bufferPool, rootNodePage, rootNodeIdx);
    markDirty(treeMgmt->bufferPool, rootNodePage);

    BT_BtreeNode *rootNode = readTreeNodePage(rootNodePage);

    int idx = 0;
    res = dfs(tree, rootNode, &idx);

    free(rootNodePage);
    free(rootNode);
    return res;
}

RC closeTreeScan (BT_ScanHandle *handle) {
    free(handle->mgmtData);
    free(handle);
    return RC_OK;
}

