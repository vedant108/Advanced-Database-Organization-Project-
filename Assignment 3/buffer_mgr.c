//#ifndef BUFFER_LIST_H_INCLUDED
#define BUFFER_LIST_H_INCLUDED
#include "buffer_mgr.h"

#include "storage_mgr.h"
#include "dt.h"
#include <stdio.h>
#include <stdlib.h>

#include "buffer_mgr.h"


typedef struct BufferPool_Entry
{
    void *buffer_pool_ptr;
    void *buffer_page_info;
    int numreadIO;
    int numwriteIO;
    struct BufferPool_Entry *nextBufferEntry;
} BufferPool_Entry, *EntryPointer;

typedef struct Buffer_page_info
{
    char *pageframes;
    PageNumber pagenums;
    bool isdirty;
    int fixcounts;
    int weight;
    long double timeStamp;
} Buffer_page_info;


RC insert_bufpool(EntryPointer *entry, void *buffer_pool_ptr, void * buffer_page_dtl);
BufferPool_Entry *find_bufferPool(EntryPointer entryptr, void * buffer_pool_ptr);
bool delete_bufpool(EntryPointer *entryptr, void *buffer_pool_ptr);
BufferPool_Entry *checkPoolsUsingFile(EntryPointer entry, int *filename);
int getPoolsUsingFile(EntryPointer entry, char *filename);


static EntryPointer entry_ptr_bp = NULL;
static long double time_uni = -32674;
static char *initFrames(const int numPages);
static Buffer_page_info *findReplace(BM_BufferPool *const bm, BufferPool_Entry *entry_bp);
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC initBufferPool(BM_BufferPool *const bm, const char *const pg_file_name, const int numPages, ReplacementStrategy strategy, void *stratData)
{

    int i;
    bool insert_ok;
    Buffer_page_info *buffer_pg_info;
    BufferPool_Entry *buffer_entry;
    BM_BufferPool *pool_temp;
    SM_FileHandle *fileHandle;

    buffer_entry = checkPoolsUsingFile(entry_ptr_bp, pg_file_name);                                             // Check for the buffer pools using the same file.

    if(buffer_entry != NULL)                                                                                    // Create a buffer pool if does not exists
    {
        pool_temp = buffer_entry->buffer_pool_ptr;
        bm->pageFile = pg_file_name;
        bm->numPages = numPages;
        bm->strategy = strategy;
        bm->mgmtData = pool_temp->mgmtData;
        buffer_pg_info = buffer_entry->buffer_page_info;
        return insert_bufpool(&entry_ptr_bp, bm, buffer_pg_info);
    }
    else
    {
        fileHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
        bm->numPages = numPages;
        bm->strategy = strategy;
        bm->pageFile = pg_file_name;
        bm->mgmtData = fileHandle;
        openPageFile(pg_file_name,fileHandle);
        buffer_pg_info = (Buffer_page_info *)calloc(numPages,sizeof(Buffer_page_info));
        i=0;
        while(i < numPages)
        {
            (buffer_pg_info+i)->pageframes = initFrames(numPages);
            buffer_pg_info[i].fixcounts = 0;
            buffer_pg_info[i].isdirty = FALSE;
            buffer_pg_info[i].pagenums = NO_PAGE;
            i++;
        }
        return insert_bufpool(&entry_ptr_bp,bm,buffer_pg_info);
    }
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
static char *initFrames(const int numPages)
{
    int i=0;
    char *frame;
    frame = (char *)malloc(PAGE_SIZE);
    
    if(frame!=NULL)
    {
        while(i < PAGE_SIZE)
        {
            frame[i] = '\0';
            i++;
        }
    }
    return frame;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    int *fixCnt = getFixCounts(bm);
    int i=0;
    while(i < bm->numPages)
    {
        if (!(*(i + fixCnt)))
            i++;
        else
        {
            free(fixCnt);
            return RC_SHUTDOWN_POOL_FAILED;
        }
    }

    RC RCflg = forceFlushPool(bm);
    if (RCflg == RC_OK)
    {
        freePagesForBuffer(bm);
        free(fixCnt);
        free(bm->mgmtData);
        return RC_OK;
    }
    else
    {
        free(fixCnt);
        return RCflg;
    }
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC forceFlushPool(BM_BufferPool *const bm)
{
    BM_PageHandle* page;
    RC RCflg;

    bool *dFlg = getDirtyFlags(bm);
    int *fixCnt = getFixCounts(bm);

    int i =0;
    while (i < bm->numPages)
    {
        if (*(dFlg + i) == 1)
        {
            if (!(*(fixCnt + i)))
            {
                page = ((bm->mgmtData) + i);
                RCflg = forcePage(bm, page);
                if (RCflg != RC_OK)
                {
                    free(dFlg);
                    free(fixCnt);
                    return RCflg;
                }
            }
            else
                continue;
        }
        i++;
    }
    i = 0;
    while (i < bm->numPages)
    {
        if (*(dFlg + i))
            ((bm->mgmtData) + i)->dirty = 0;
        i++;
    }

    free(dFlg);
    free(fixCnt);
    return RC_OK;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    PageNumber *pageNoArray = (PageNumber*)malloc(bm->numPages * sizeof(PageNumber));
    int i = 0;
    while (i < bm->numPages)
    {
        if ((bm->mgmtData + i)->data != NULL)
            pageNoArray[i] = (bm->mgmtData+i)->pageNum;
        else
            pageNoArray[i] = NO_PAGE;
        i++;
    }
    return pageNoArray;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
int *getFixCounts (BM_BufferPool *const bm)
{
    int i = 0;
    int *pageNoArray = (int*)malloc(bm->numPages * sizeof(int));
    while (i < bm->numPages)
    {
        pageNoArray[i] = (bm->mgmtData + i) -> fixCounts;
        i++;
    }
    return pageNoArray;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
bool *getDirtyFlags (BM_BufferPool *const bm)
{
    bool *pageNoArray = (bool*)malloc(bm->numPages * sizeof(bool));
    for (int i = 0; i < bm->numPages; i++)
        pageNoArray[i] = (bm->mgmtData + i) -> dirty;
    return pageNoArray;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
int getNumReadIO (BM_BufferPool *const bm)
{
    int read = bm->numberReadIO;
    return read;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
int getNumWriteIO (BM_BufferPool *const bm)
{
    int write = bm->numberWriteIO;
    return write;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page)
{
    int i = 0;
    BufferPool_Entry *page_entry;
    Buffer_page_info *buffer_pg_info;

    page_entry = find_bufferPool(entry_ptr_bp, bm);
    buffer_pg_info = page_entry->buffer_page_info;

    while(i < bm->numPages)
    {

        if (((buffer_pg_info + i)->pagenums) == page->pageNum)
        {
            (buffer_pg_info + i)->isdirty = TRUE;
            return RC_OK;
        }
        i++;
    }

    return RC_MARK_DIRTY_FAILED;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{


    FILE *f = fopen(bm->pageFile, "rb+");

    long size = (page->pageNum)*PAGE_SIZE;
    int j = 0;
    fseek(f, size, SEEK_SET);
    fwrite(page->data, PAGE_SIZE, 1, f);
    bm->numberWriteIO = bm->numberWriteIO + 1;
    fclose(f);
    int i=0;
    while(i < bm->numPages)
    {
        bool dirt = (bm->mgmtData + i)->dirty;
        int pageno = (bm->mgmtData + i)->pageNum;
        if (pageno != page->pageNum)
            i++;
        else
        {
            dirt = 0;
            break;
        }
    }
    page->dirty = j;
    return RC_OK;
}

//--------------------------------------------------------------------------------------------------------------------------------------------------
RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page)
{
    int i = 0, tot_pages = bm->numPages;;

    BufferPool_Entry *entry_bp;
    Buffer_page_info *page_info;

    entry_bp = find_bufferPool(entry_ptr_bp, bm);
    page_info = entry_bp->buffer_page_info;

    while(i < tot_pages)
    {
        if ((page_info + i)->pagenums == page->pageNum)
        {
            (page_info + i)->fixcounts = (page_info + i)->fixcounts -  1;
            return RC_OK;
        }
        i++;
    }
    return RC_UNPIN_FAILED;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum)
{
    int i = 0, emptyFlag = 1;
    RC pin_ok = RC_OK;
    Buffer_page_info *rep_possible, *page_info;
    BufferPool_Entry *entry_bp;

    entry_bp = find_bufferPool(entry_ptr_bp, bm);
    page_info = entry_bp->buffer_page_info;

    if (page_info != NULL)
    {
        while(i < bm->numPages)
        {
            emptyFlag = 0;
            if ((page_info + i)->pagenums == pageNum)
            {

                (page_info + i)->timeStamp = time_uni++;                                                      // LRU only
                page->pageNum = pageNum;
                page->data = (page_info + i)->pageframes;
                (page_info + i)->fixcounts = (page_info + i)->fixcounts + 1;
                return RC_OK;
            }
            i++;
        }

        i=0;
        while(i < bm->numPages)
        {
            if ((page_info + i)->pagenums == -1)
            {
                rep_possible = ((page_info + i));
                emptyFlag = 1;
                break;
            }
            i++;
        }
    }

    if (emptyFlag != 1)
    {
        if (bm->strategy == RS_FIFO)
            return applyFIFO(bm, page, pageNum);
        else if (bm->strategy == RS_LRU)
            return applyLRU(bm, page, pageNum);
        else if (bm->strategy == RS_LRU)
            return applyLFU(bm, page, pageNum);
        else
            return RC_PIN_FAILED;
    }
    else
    {
        page->pageNum = pageNum;
        page->data = rep_possible->pageframes;

        pin_ok = readBlock(pageNum, bm->mgmtData,rep_possible->pageframes);
        if((pin_ok == RC_READ_NON_EXISTING_PAGE) || (pin_ok == RC_OUT_OF_BOUNDS))
            pin_ok = appendEmptyBlock(bm->mgmtData);
        else
            pin_ok = RC_OK;
        entry_bp->numreadIO++;
        rep_possible->fixcounts = rep_possible->fixcounts + 1;
        rep_possible->pagenums = pageNum;
        return pin_ok;
    }
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC applyFIFO(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum) 
{
    BufferPool_Entry *entry_bp;
    entry_bp = find_bufferPool(entry_ptr_bp, bm);
    Buffer_page_info *rep_possible = NULL;
   
    rep_possible = findReplace(bm, entry_bp);
    
    RC write_ok = RC_OK;
    RC read_ok = RC_OK;
    
    if (rep_possible->isdirty == TRUE) 
    {
        write_ok = writeBlock(rep_possible->pagenums, bm->mgmtData, rep_possible->pageframes);
        rep_possible->isdirty = FALSE;
        entry_bp->numwriteIO++;
    }
    
    read_ok = readBlock(pageNum, bm->mgmtData, rep_possible->pageframes);
    if((read_ok == RC_READ_NON_EXISTING_PAGE) || (read_ok == RC_OUT_OF_BOUNDS) || (read_ok == RC_READ_FAILED))
        read_ok = appendEmptyBlock(bm->mgmtData);

    entry_bp->numreadIO++;
    page->pageNum  = pageNum;
    page->data = rep_possible->pageframes;
    rep_possible->pagenums = pageNum;
    rep_possible->fixcounts = rep_possible->fixcounts + 1;
    rep_possible->weight = rep_possible->weight + 1;

    if((read_ok == RC_OK) && (write_ok == RC_OK))
        return RC_OK;
    else
        return RC_FIFO_FAILED;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC applyLRU(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum)
 {
    BufferPool_Entry *entry_bp;
    entry_bp = find_bufferPool(entry_ptr_bp, bm);
    Buffer_page_info *rep_possible;
    rep_possible = findReplace(bm, entry_bp);

    RC write_ok = RC_OK;
    RC read_ok = RC_OK;

    if (rep_possible->isdirty == TRUE) 
    {
        write_ok = writeBlock(page->pageNum, bm->mgmtData, rep_possible->pageframes);
        rep_possible->isdirty = FALSE;
        entry_bp->numwriteIO = entry_bp->numwriteIO + 1;
    }
    
    read_ok = readBlock(pageNum, bm->mgmtData, rep_possible->pageframes);
    entry_bp->numreadIO++;
    page->pageNum  = pageNum;
    page->data = rep_possible->pageframes;
    rep_possible->pagenums = pageNum;
    rep_possible->fixcounts = rep_possible->fixcounts +1;
    rep_possible->weight = rep_possible->weight + 1;
    rep_possible->timeStamp = (long double)time_uni++;

    if ((read_ok == RC_OK) && (write_ok == RC_OK))
        return RC_OK;
    else
        return RC_LRU_FAILED;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC applyLFU(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum)
{
    BufferPool_Entry *entry_bp;
    entry_bp = find_bufferPool(entry_ptr_bp, bm);
    Buffer_page_info *rep_possible=NULL;
    rep_possible = findReplace(bm, entry_bp);

    RC write_ok = RC_OK;

    if (rep_possible->isdirty == TRUE) 
    {
        write_ok = writeBlock(rep_possible->pagenums, bm->mgmtData, rep_possible->pageframes);
        rep_possible->isdirty = FALSE;
        entry_bp->numwriteIO = entry_bp->numwriteIO + 1;
    }

    RC read_ok = readBlock(pageNum, bm->mgmtData, rep_possible->pageframes);
    if((read_ok == RC_READ_NON_EXISTING_PAGE) || (read_ok == RC_OUT_OF_BOUNDS) || (read_ok == RC_READ_FAILED))
        read_ok = appendEmptyBlock(bm->mgmtData);
    entry_bp->numreadIO = entry_bp->numreadIO + 1;
    page->pageNum  = pageNum;
    page->data = rep_possible->pageframes;
    rep_possible->pagenums = pageNum;
    rep_possible->fixcounts = rep_possible->fixcounts + 1;
    rep_possible->weight = rep_possible->weight + 1;
    
    if ((read_ok == RC_OK) && (write_ok == RC_OK))
        return RC_OK;
    else
        return RC_LFU_FAILED;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
Buffer_page_info *findReplace(BM_BufferPool * const bm, BufferPool_Entry *entry_bp) 
{   
    int i=0, cnt = 0;
    
    Buffer_page_info *bf_page_info = entry_bp->buffer_page_info, *bf_page_info_zeros[bm->numPages];

    while(i< bm->numPages)
    {
        bf_page_info_zeros[i] = NULL;
        i++;
    }
    
    i=0;
    while(i < bm->numPages)
    {
        if ((bf_page_info[i].fixcounts) == 0) 
        {
            bf_page_info_zeros[cnt] = (bf_page_info+i);
            cnt++;
        }
        i++;
    }
   
    #define sizeofa(array) sizeof array / sizeof array[0]
    Buffer_page_info *next_bf_page_info, *replace_bf_page_info;

    replace_bf_page_info = bf_page_info_zeros[0];
    i=0;
    while(i < sizeofa(bf_page_info_zeros))
     {
        next_bf_page_info = bf_page_info_zeros[i];
        if(next_bf_page_info != NULL)
            if(bm->strategy == RS_FIFO)
                if ((replace_bf_page_info->weight) > (next_bf_page_info->weight))
                    replace_bf_page_info = next_bf_page_info;
            else if(bm->strategy == RS_LRU)
                if(replace_bf_page_info->timeStamp > next_bf_page_info->timeStamp)
                    replace_bf_page_info = next_bf_page_info;
            else if (bm->strategy == RS_LFU)
                if ((replace_bf_page_info->weight) > (next_bf_page_info->weight))
                    replace_bf_page_info = next_bf_page_info;
        i++;
    }
    return replace_bf_page_info;
}
RC insert_bufpool(EntryPointer *entry, void *buffer_pool_ptr, void *buffer_page_handle)
{
    EntryPointer prvs, newptr, crnt;

    newptr = (EntryPointer)malloc(sizeof(BufferPool_Entry));
    if(newptr != NULL)
    {
        newptr->buffer_pool_ptr = buffer_pool_ptr;
        newptr->buffer_page_info = buffer_page_handle;
        newptr->numreadIO = 0;
        newptr->numwriteIO = 0;
        newptr->nextBufferEntry = NULL;

        prvs = NULL;
        crnt = *entry;

        while (crnt != NULL )
        {
            prvs = crnt;
            crnt = crnt->nextBufferEntry;
        }
        if(prvs != NULL)
            prvs->nextBufferEntry = newptr;
        else
            *entry = newptr;
        return RC_OK;
    }
    return RC_INSERT_BUFPOOL_FAILED;
}
//-------------------------------------------------------------------------------------------------
BufferPool_Entry *find_bufferPool(EntryPointer entryptr, void *buffer_pool_ptr)
{
    EntryPointer crnt = entryptr;
    while(crnt != NULL)
    {
        if(crnt->buffer_pool_ptr == buffer_pool_ptr)
            break;
        crnt = crnt->nextBufferEntry;
    }
    return crnt;
}
//-------------------------------------------------------------------------------------------------
bool delete_bufpool(EntryPointer *entryptr, void *buffer_pool_ptr )
{
    EntryPointer temp, prvs, crnt;

    prvs = NULL;
    crnt = *entryptr;

    while((crnt->buffer_pool_ptr != buffer_pool_ptr) && (crnt != NULL))
    {
        prvs = crnt;
        crnt = crnt->nextBufferEntry;
    }

    temp = crnt;

    if(prvs != NULL)
        prvs->nextBufferEntry = crnt->nextBufferEntry;
    else
        *entryptr = crnt->nextBufferEntry;
    free(temp);
    return TRUE;
}
//-------------------------------------------------------------------------------------------------
BufferPool_Entry *checkPoolsUsingFile(EntryPointer entry, int *filename)
{
    EntryPointer crnt = entry;
    BM_BufferPool *bufferpool;
    while(crnt!=NULL)
    {
        bufferpool = (BM_BufferPool *)crnt->buffer_pool_ptr;
        if(bufferpool->pageFile == filename)
            break;
        crnt = crnt->nextBufferEntry;
    }
    return crnt;
}
//-------------------------------------------------------------------------------------------------
int getPoolsUsingFile(EntryPointer entry, char *filename)
{
    EntryPointer crnt=entry;
    BM_BufferPool *bufferpool;

    int cnt = 0;
    while(crnt != NULL)
    {
        bufferpool = (BM_BufferPool *)crnt->buffer_pool_ptr;
        if(bufferpool->pageFile == filename)
            cnt++;
        crnt = crnt->nextBufferEntry;
    }
    return cnt;
}

//--------------------------------------------------------------------------------------------------------------------------------------------------
void freePagesForBuffer(BM_BufferPool *bm)
{
    int i=0;
    while (i < bm->numPages)
    {
        free((i + bm->mgmtData)->data);
        free((i + bm->mgmtData)->strategyAttribute);
        i++;
    }
}
