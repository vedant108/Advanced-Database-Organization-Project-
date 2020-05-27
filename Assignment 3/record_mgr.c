#define RECORD_SCAN_H
#include "storage_mgr.h"

#include "record_mgr.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "buffer_mgr.h"
#include "record_mgr.h"
#include <stdio.h>
#include <stdlib.h>

typedef struct AUX_Scan
{
    BM_PageHandle *pHandle;
    int _sPage;
    int _slotID;
    int _recLength;
    int _recsPage;
    int _numPages;
}AUX_Scan;

typedef struct Scan_Entry
{
    RM_ScanHandle *sHandle;
    AUX_Scan *auxScan;
    struct Scan_Entry *nextScan;
} Scan_Entry,*PTR_Scan;
//--------------------------------------------------------------------------
AUX_Scan *search(RM_ScanHandle *sHandle, PTR_Scan sEntry);
RC insert(RM_ScanHandle *sHandle, PTR_Scan *sEntry, AUX_Scan *auxScan);
RC delete(RM_ScanHandle *sHandle, PTR_Scan *sEntry);
static Schema glbal_schma;
static PTR_Scan scanning_ptr = NULL;

RC initRecordManager(void *mgmtData)
{
    return RC_OK;
}

RC shutdownRecordManager()
{
    return RC_OK;
}

RC createTable (char *name, Schema *schema)
{
    char lcl_filenm[64] = {'\0'};
    strcat(lcl_filenm,name);
    strcat(lcl_filenm,".bin");
    createPageFile(lcl_filenm);
    glbal_schma = *schema;
    return RC_OK;
}

RC openTable (RM_TableData *rel, char *name)
{
    char lcl_filenm[64] = {'\0'};
    void *schema_size = malloc(sizeof(Schema));
    void *bufferPoolSize = malloc (sizeof(BM_BufferPool));
    Schema *lcl_schma = ((Schema *)schema_size);
    BM_BufferPool *bffrpl = ((BM_BufferPool *) bufferPoolSize);

    strcat(lcl_filenm,name);
    strcat(lcl_filenm,".bin");
    initBufferPool(bffrpl,lcl_filenm,4,RS_FIFO,NULL);
    rel->name = name;
    rel->mgmtData = bffrpl;
    rel->schema = lcl_schma;
    *lcl_schma = glbal_schma;

    return RC_OK;
}

RC closeTable (RM_TableData *rel)
{
    void *mgmtdata=rel->mgmtData;
    BM_BufferPool *bffrpl;
    bffrpl=(BM_BufferPool *)mgmtdata;
    shutdownBufferPool(bffrpl);
    free(bffrpl);
    return RC_OK;
}





RC insertRecord (RM_TableData *rel, Record *record)
{
    int slt_num = 0;
    int pg_len;
    char *spc = NULL;
    RID id;
    int ttl_record_len;
    BM_PageHandle *pg_hndl;
    PageNumber pg_num;
    BM_BufferPool *bffrpl;
    SM_FileHandle *s_hndl;
    void *pagehandle_Size = malloc(sizeof(BM_PageHandle));




    bffrpl=(BM_BufferPool *)rel->mgmtData;

    pg_hndl= (BM_PageHandle *) pagehandle_Size;
    s_hndl=(SM_FileHandle *)bffrpl->mgmtData;
    pg_num=1;

    ttl_record_len = getRecordSize(rel->schema);
    for(pg_num=1;pg_num < s_hndl->totalNumPages;pg_num++)
    {


        pinPage(bffrpl,pg_hndl,pg_num);
        pg_len = strlen(pg_hndl->data);


        if(PAGE_SIZE-pg_len < ttl_record_len)
        {
            unpinPage(bffrpl,pg_hndl);
        }
        else if(PAGE_SIZE-pg_len > ttl_record_len){
            int size = pg_len/ttl_record_len;

            slt_num = size;
            unpinPage(bffrpl,pg_hndl);
            break;

        }


    }


    if(slt_num == 0)
    {
        pinPage(bffrpl,pg_hndl,pg_num + 1);
        unpinPage(bffrpl,pg_hndl);
    }
    pinPage(bffrpl,pg_hndl,pg_num);
    int pageDataLength = strlen(pg_hndl->data);
    spc = pg_hndl->data + pageDataLength;

    strcpy(spc,record->data);
    markDirty(bffrpl,pg_hndl);
    unpinPage(bffrpl,pg_hndl);
    id.page = pg_num;
    id.slot = slt_num;
    record->id = id;
    return RC_OK;
}
int getNumTuples (RM_TableData *rel)
{

    int tples_cnt = 0;
    void *pageHandleSize= malloc (sizeof(BM_PageHandle));

    PageNumber pg_num = 1;
    BM_PageHandle *pghndl;
    BM_BufferPool *bffrpl;
    SM_FileHandle *flehndl;

    pghndl=((BM_PageHandle *) pageHandleSize);

    bffrpl= (BM_BufferPool *)rel->mgmtData;
    void *mgmt = bffrpl->mgmtData;

    flehndl= (SM_FileHandle *)mgmt;
    PageNumber numPages = flehndl->totalNumPages;
    while(pg_num < numPages)
    {
        pinPage(bffrpl,pghndl,pg_num);
        pg_num++;
        int i;
        for(i=0;i < PAGE_SIZE;i++)
        {
            if(tples_cnt=pghndl->data[i] == '-')
                tples_cnt++;

        }
    }
    return tples_cnt;
}


RC deleteRecord (RM_TableData *rel, RID id)
{
    BM_PageHandle *pg_hndl;
    BM_BufferPool *bffrpl;
    PageNumber pg_num = 0;
    void *value = rel->mgmtData;
    bffrpl = (BM_BufferPool *)value;
    pg_hndl=MAKE_PAGE_HANDLE();
    pg_num = id.page;
    pinPage(bffrpl,pg_hndl,pg_num);
    markDirty(bffrpl,pg_hndl);
    unpinPage(bffrpl,pg_hndl);
    free(pg_hndl);

    return RC_OK;
}




RC updateRecord (RM_TableData *rel, Record *record)
{
    RID id;
    BM_PageHandle *pg_hndl;
    BM_BufferPool *bffrpl ;
    char *spc;
    int rcrd_len;
    int slt_num;
    PageNumber pg_num;
    bffrpl= (BM_BufferPool *)rel->mgmtData;
    pg_hndl = MAKE_PAGE_HANDLE();
    id = record->id;
    slt_num = id.slot;
    pg_num = id.page;


    pinPage(bffrpl,pg_hndl,pg_num);

    rcrd_len = getRecordSize(rel->schema);
    int recordandslotSize = rcrd_len*slt_num;

    spc = (pg_hndl->data + recordandslotSize);
    strncpy(spc,record->data,rcrd_len);
    return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{

    void *sizeofpagehandle;
    int slt_num = id.slot;

    char *spc;

    BM_BufferPool *bffrpl;
    int rcrd_len;


    BM_PageHandle *pg_hndl;
    sizeofpagehandle=malloc (sizeof(BM_PageHandle));
    pg_hndl= ((BM_PageHandle *)sizeofpagehandle);

    bffrpl= (BM_BufferPool *)rel->mgmtData;


    PageNumber pg_num = id.page;
    pinPage(bffrpl,pg_hndl,pg_num);
    rcrd_len = getRecordSize(rel->schema);

    spc = pg_hndl->data + rcrd_len * slt_num;
    strncpy(record->data,spc,rcrd_len);
    unpinPage(bffrpl,pg_hndl);
    return RC_OK;
}


RC next (RM_ScanHandle *scan, Record *record){
    RID id;
    Expr *le,*re,*a_exprsn,*exprsn;
    RM_TableData *rdta;
    AUX_Scan *AUX_Scan;
    exprsn = (Expr *)scan->mgmtData;
    rdta = scan->rel;
    Operator *dec,*dec2;

    AUX_Scan= search(scan,scanning_ptr);
    void *size = malloc(sizeof(Value *));

    Value **cValue = (Value **)malloc(size);
    *cValue = NULL;
    dec = exprsn->expr.op;

    switch(dec->type)
    {
        case OP_COMP_SMALLER:
        {

            for(AUX_Scan->_sPage=0;AUX_Scan->_sPage<AUX_Scan->_numPages;AUX_Scan->_slotID++)
            {
                le = dec->args[0];
                re = dec->args[1];
                int aux_length=AUX_Scan->_recLength;
                le = dec->args[0];
                re = dec->args[1];
                pinPage(rdta->mgmtData,AUX_Scan->pHandle,AUX_Scan->_sPage);
                int recsPageLength ;
                recsPageLength= strlen(AUX_Scan->pHandle->data) / aux_length;
                AUX_Scan->_recsPage = recsPageLength;
                for(AUX_Scan->_sPage=0;AUX_Scan->_sPage<AUX_Scan->_numPages;AUX_Scan->_slotID++)
                {
                    id.slot = AUX_Scan->_slotID;
                    id.page = AUX_Scan->_sPage;

                    getRecord(rdta,id,record);
                    getAttr(record,rdta->schema,re->expr.attrRef,cValue);
                    if((rdta->schema->dataTypes[re->expr.attrRef] == DT_INT) && (le->expr.cons->v.intV>cValue[0]->v.intV))
                    {

                        unpinPage(rdta->mgmtData,AUX_Scan->pHandle);
                        return RC_OK;
                    }

                }
                break;
            }
            break;
        }
        case OP_COMP_EQUAL:
        {

            for(AUX_Scan->_sPage=0;AUX_Scan->_sPage<AUX_Scan->_numPages;AUX_Scan->_slotID++)

            {
                int aux_length=AUX_Scan->_recLength;
                le = dec->args[0];
                re = dec->args[1];
                pinPage(rdta->mgmtData,AUX_Scan->pHandle,AUX_Scan->_sPage);
                int recsPageLength ;
                recsPageLength= strlen(AUX_Scan->pHandle->data) / aux_length;
                AUX_Scan->_recsPage = recsPageLength;
                for(AUX_Scan->_sPage=0;AUX_Scan->_sPage<AUX_Scan->_numPages;AUX_Scan->_slotID++)
                {
                    id.slot = AUX_Scan->_slotID;
                    id.page = AUX_Scan->_sPage;


                    getRecord(rdta,id,record);
                    getAttr(record,rdta->schema,re->expr.attrRef,cValue);
                    if(rdta->schema->dataTypes[re->expr.attrRef] == DT_STRING)
                    {
                        if(strcmp(cValue[0]->v.stringV , le->expr.cons->v.stringV) == 0) {

                            unpinPage(rdta->mgmtData, AUX_Scan->pHandle);
                            return RC_OK;
                        }
                    }
                    else if(rdta->schema->dataTypes[re->expr.attrRef] == DT_INT)
                    {
                        if(cValue[0]->v.intV == le->expr.cons->v.intV) {

                            unpinPage(rdta->mgmtData, AUX_Scan->pHandle);
                            return RC_OK;
                        }
                    }
                    else if(cValue[0]->v.floatV == le->expr.cons->v.floatV)
                    {
                        if(rdta->schema->dataTypes[re->expr.attrRef] == DT_FLOAT) {

                            unpinPage(rdta->mgmtData, AUX_Scan->pHandle);
                            return RC_OK;
                        }
                    }

                }
                break;
            }
            break;
        }
        case OP_BOOL_NOT:
        {
            a_exprsn = exprsn->expr.op->args[0];
            dec2 = a_exprsn->expr.op;

            if (dec2->type == OP_COMP_SMALLER)
            {
                for(AUX_Scan->_sPage=0;AUX_Scan->_sPage<AUX_Scan->_numPages;AUX_Scan->_slotID++)
                {
                    int aux_length=AUX_Scan->_recLength;
                    re = dec2->args[0];
                    le = dec2->args[1];
                    pinPage(rdta->mgmtData,AUX_Scan->pHandle,AUX_Scan->_sPage);
                    int recsPageLength ;
                    recsPageLength= strlen(AUX_Scan->pHandle->data) / aux_length;
                    AUX_Scan->_recsPage = recsPageLength;
                    for(AUX_Scan->_sPage=0;AUX_Scan->_sPage<AUX_Scan->_numPages;AUX_Scan->_slotID++)
                    {
                        id.page = AUX_Scan->_sPage;
                        id.slot = AUX_Scan->_slotID;

                        getRecord(rdta,id,record);
                        getAttr(record,rdta->schema,re->expr.attrRef,cValue);
                        if((cValue[0]->v.intV > le->expr.cons->v.intV) && (rdta->schema->dataTypes[re->expr.attrRef] == DT_INT))
                        {

                            unpinPage(rdta->mgmtData,AUX_Scan->pHandle);
                            return RC_OK;
                        }

                    }
                    break;
                }
                break;
            }
            break;
        }
//        case OP_BOOL_AND:
        case OP_BOOL_OR:
            break;
    }
    return RC_RM_NO_MORE_TUPLES;
}

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{

    AUX_Scan *auxillary_scan;
    BM_BufferPool *bffrpl;
    SM_FileHandle *flehndl;
    int total_numberofpages =flehndl->totalNumPages;
    int records_length = getRecordSize(rel->schema);

    void *value_aux = malloc(sizeof(AUX_Scan));
    auxillary_scan=(AUX_Scan *)value_aux;

    bffrpl= (BM_BufferPool *)rel->mgmtData;

    flehndl = (SM_FileHandle *)bffrpl->mgmtData;


    auxillary_scan->_slotID = 1;
    auxillary_scan->_sPage = 1;
    auxillary_scan->_numPages = total_numberofpages;

    auxillary_scan->pHandle = ((BM_PageHandle *) malloc (sizeof(BM_PageHandle)));
    auxillary_scan->_recLength = records_length;




    scan->mgmtData = cond;
    scan->rel = rel;

    free(bffrpl);
    free(flehndl);



    return insert(scan,&scanning_ptr,auxillary_scan);
}




RC closeScan (RM_ScanHandle *scan)
{
    search(scan,scanning_ptr);
    return delete(scan,&scanning_ptr);
}

RC deleteTable (char *name)
{
    char lcl_filenm[50] = {'\0'};
    strcat(lcl_filenm,name);
    strcat(lcl_filenm,".bin");
    destroyPageFile(lcl_filenm);
    return RC_OK;
}



int getRecordSize(Schema *schema)
{
    int siz=0;
    int var_size=0;
    int *typ_len = schema->typeLength;
    int i = 0;
    while(i < schema->numAttr)
    {
        switch(schema->dataTypes[i])
        {

            case DT_BOOL:
                var_size = sizeof(bool);
                siz = siz + var_size;
                break;

            case DT_INT:
                var_size = sizeof(int);
                siz = siz +var_size;
                break;

            case DT_FLOAT:
                var_size = sizeof(float);
                siz  = siz + var_size;
                break;


            case DT_STRING:
                siz = siz + typ_len[i];
                break;
        }
        i++;
    }
    return siz+schema->numAttr;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,int *typeLength, int keySize, int *keys)
{

    void *schemaSize=malloc(sizeof(Schema));
    Schema *schma = (Schema*) schemaSize;
    schma->typeLength = typeLength;
    int attribute_number = numAttr;
    schma->numAttr = attribute_number;
    schma->keySize = keySize;
    schma->attrNames = attrNames;
    schma->dataTypes = dataTypes;


    schma->keyAttrs = keys;
    return schma;
}


RC createRecord(Record **record, Schema *schema)
{
    void *recordSize=malloc(sizeof(Record));

    Record *rec = ((Record*)recordSize);
    int siz = getRecordSize(schema);
    void *schemaSize=malloc(siz);
    rec->data = ((char*)schemaSize) ;
    memset(rec->data, 0, siz);
    *record = rec;
    return (RC_OK);
}


RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{

    int var_size=0;
    DataType *dta_typ;
    int *typ_len;
    typ_len= schema->typeLength;
    int tmpo;
    dta_typ= schema->dataTypes;
    int j;
    char *rcrd_off_st;
    int i=0;
    int off_st=0;

    for(i=0;i < attrNum;i++)
    {
        switch(dta_typ[i])
        {
            case DT_INT:
                var_size=sizeof(int);
                off_st = off_st + var_size;
                break;

            case DT_FLOAT:
                var_size=sizeof(float);
                off_st = off_st + var_size ;
                break;

            case DT_BOOL:
                var_size=sizeof(bool);
                off_st = off_st + var_size;
                break;

            case DT_STRING:
                off_st = off_st + typ_len[i];
                break;
        }

    }
    off_st = off_st + (attrNum + 1);
    rcrd_off_st = record->data;

    if(attrNum == 0)
    {
        rcrd_off_st[0] = '-';
        rcrd_off_st=rcrd_off_st+1;

    }
    else
    {
        rcrd_off_st = rcrd_off_st+ off_st;
        (rcrd_off_st - 1)[0] = ' ';

    }

    if(value->dt==DT_INT){

        sprintf(rcrd_off_st, "%d", value->v.intV);
            while(strlen(rcrd_off_st)<sizeof(int))
                strcat(rcrd_off_st,"0");
            for (j = strlen(rcrd_off_st)-1,i = 0; i < j;i++,j--){
                tmpo = rcrd_off_st[i];
                rcrd_off_st[i] = rcrd_off_st[j];
                rcrd_off_st[j] = tmpo;
            }
    }

    else if(value->dt==DT_BOOL){
        sprintf(rcrd_off_st,"%i",value->v.boolV);


    }
    else if(value->dt==DT_STRING){
        sprintf(rcrd_off_st,"%s",value->v.stringV);

    }

    else if(value->dt==DT_FLOAT){
        sprintf(rcrd_off_st,"%f",value->v.floatV);
            while(strlen(rcrd_off_st) != sizeof(float)) strcat(rcrd_off_st,"0");
            for (j = strlen(rcrd_off_st)-1,i = 0; i < j; i++,j--)
            {
                tmpo = rcrd_off_st[i]; rcrd_off_st[i]=rcrd_off_st[j];
                rcrd_off_st[j] = tmpo;
            }

    }


    return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    int variable_Size = 0;
    DataType *dta_typ;

    int *typ_len;
    int off_st = 0, i = 0;
    char *dt = record->data;
    dta_typ= schema->dataTypes;
    typ_len=schema->typeLength;
    void *DataTypeSize = malloc(sizeof(Value));
    Value *vlue = (Value *) DataTypeSize;

    while (i < attrNum) {

        if (dta_typ[i] == DT_INT) {
            variable_Size = sizeof(int);
            off_st = off_st + variable_Size;


        } else if (dta_typ[i] == DT_FLOAT) {
            variable_Size = sizeof(float);
            off_st = off_st + variable_Size;

        } else if (dta_typ[i] == DT_BOOL) {
            variable_Size = sizeof(bool);
            off_st = off_st + variable_Size;


        } else if (dta_typ[i] == DT_STRING) {
            off_st = off_st + typ_len[i];

        }
        i++;
    }


    off_st = off_st + (attrNum + 1);
    char *tmpo = NULL;

    if (dta_typ[i] == DT_INT) {
        vlue->dt = DT_INT;
        void *value = malloc(sizeof(int) + 1);
        tmpo = (char *) value;
        strncpy(tmpo, dt + off_st, sizeof(int));
        tmpo[sizeof(int)] = '\0';
        vlue->v.intV = atoi(tmpo);

    } else if (dta_typ[i] == DT_FLOAT) {
        vlue->dt = DT_FLOAT;
        tmpo = malloc(sizeof(float) + 1);
        strncpy(tmpo, dt + off_st, sizeof(float));
        tmpo[sizeof(float)] = '\0';
        vlue->v.floatV = (float) *tmpo;

    } else if (dta_typ[i] == DT_BOOL) {
        vlue->dt = DT_BOOL;
        tmpo = malloc(sizeof(bool) + 1);
        strncpy(tmpo, dt + off_st, sizeof(bool));
        tmpo[sizeof(bool)] = '\0';
        vlue->v.boolV = (bool) *tmpo;

    } else if (dta_typ[i] == DT_STRING) {
        vlue->dt = DT_STRING;
        int siz = typ_len[i];
        tmpo = malloc(siz + 1);
        strncpy(tmpo, dt + off_st, siz);
        tmpo[siz] = '\0';
        vlue->v.stringV = tmpo;


    }


    *value = vlue;
    return RC_OK;
}

RC freeRecord(Record *record)
{
    free(record);
    return RC_OK;
}


RC freeSchema(Schema *schema)
{
    free(schema);
    return (RC_OK);
}




AUX_Scan *search(RM_ScanHandle *sHandle, PTR_Scan sEntry)
{
    PTR_Scan curr_node = sEntry;
    while((curr_node != NULL) && (curr_node->sHandle != sHandle))
        curr_node = curr_node->nextScan;
    return curr_node->auxScan;
}
//------------------------------------------------------------------------------------
RC insert(RM_ScanHandle *sHandle, PTR_Scan *sEntry, AUX_Scan *auxScan)
{
    PTR_Scan prev_node = NULL, curr_node = *sEntry, new_node = (Scan_Entry *)malloc(sizeof(Scan_Entry));

    new_node->auxScan = auxScan;
    new_node->sHandle = sHandle;

    while(curr_node != NULL)
    {
        prev_node = curr_node;
        curr_node = curr_node->nextScan;
    }

    if(prev_node != NULL)
        prev_node->nextScan = new_node;
    else
        *sEntry = new_node;

    return RC_OK;
}
//------------------------------------------------------------------------------------
RC delete(RM_ScanHandle *sHandle, PTR_Scan *sEntry)
{
    PTR_Scan curr_node = *sEntry;
    if(curr_node != NULL)
        *sEntry = curr_node->nextScan;
    return RC_OK;
}
//---