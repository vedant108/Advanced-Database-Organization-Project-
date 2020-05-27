#include "buffer_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include "storage_mgr.h"



RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
        const int numPages, ReplacementStrategy strategy,void *stratData){

    int i=0;
    FILE *f ;
    f=fopen(pageFileName, "r+");

    if (f == NULL)
        return RC_FILE_NOT_FOUND;
    else{


        bm->pageFile = (char *)pageFileName;
        bm->strategy = strategy;
        bm->numPages = numPages;
        void *pageSize=calloc(numPages, sizeof(BM_PageHandle));
        BM_PageHandle* buffer = (BM_PageHandle *)pageSize;
        bm->mgmtData = buffer;
        while(i < numPages)
        {
            (i + bm->mgmtData)->dirty = 0;
            (i + bm->mgmtData)->fixCounts = 0;
            (i + bm->mgmtData)->data = NULL;
            (i + bm->mgmtData)->pageNum = -1;
            i++;
        }
         bm->buffertimer = 0;
         bm->numberReadIO = 0;
         bm->numberWriteIO = 0;
         fclose(f);
         return RC_OK;    
    } 

}


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

RC forceFlushPool(BM_BufferPool *const bm)
{
    BM_PageHandle* page;
    RC RCflg;
    bool *pageNoArray = (bool*)malloc(bm->numPages * sizeof(bool));
    for (int i = 0; i < bm->numPages; i++)
        pageNoArray[i] = (bm->mgmtData + i) -> dirty;

    int j = 0;
    int *fixCnt = (int*)malloc(bm->numPages * sizeof(int));
    while (j < bm->numPages)
    {
        fixCnt[j] = (bm->mgmtData + j) -> fixCounts;
        j++;
    }

    int i =0;
    while (i < bm->numPages)
    {
        if (*(pageNoArray + i) == 1)
        {
            if (!(*(fixCnt + i)))
            {
                page = ((bm->mgmtData) + i);
                RCflg = forcePage(bm, page);
                if (RCflg != RC_OK)
                {
                    free(pageNoArray);
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
        if (*(pageNoArray + i))
            ((bm->mgmtData) + i)->dirty = 0;
        i++;
    }

    free(pageNoArray);
    free(fixCnt);
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm) 
{
    int j = 0;
    int *fixCnt = (int*)malloc(bm->numPages * sizeof(int));
    while (j < bm->numPages)
    {
        fixCnt[j] = (bm->mgmtData + j) -> fixCounts;
        j++;
    }

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


RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int i=0;
    while(i < (bm->numPages))
    {
        if ((i+ bm->mgmtData)->pageNum == page->pageNum)
        {
            page->dirty = 1;
            (i + bm->mgmtData)->dirty = 1;
            break;
        }
        i++;
    }
    return RC_OK;
}


RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{


    FILE *f = fopen(bm->pageFile, "rb+");

    long size = (page->pageNum)*PAGE_SIZE;
    int j = 0;
    fseek(f, size, SEEK_SET);
    fwrite(page->data, PAGE_SIZE, 1, f);
    bm->numberWriteIO++;
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


RC AttributeUpdate(BM_BufferPool *bm, BM_PageHandle *pageHandle)
{
    if (pageHandle->strategyAttribute == 0 )
    {
        if (bm->strategy == RS_LRU )
            pageHandle->strategyAttribute = calloc(1, sizeof(int));

        else if (bm->strategy == RS_FIFO) {
            pageHandle->strategyAttribute = calloc(1, sizeof(int));
        }
    }

    if (bm->strategy == RS_LRU )                           // Assigning the number
    {
        int *atrri;
        atrri = (int*) pageHandle -> strategyAttribute;
        *atrri = (bm -> buffertimer);
        bm -> buffertimer = bm -> buffertimer + 1;
        return RC_OK;
    }
    if (bm->strategy == RS_FIFO)                          // Assigning the number
    {
        int *atrri;
        atrri = (int*) pageHandle -> strategyAttribute;
        *atrri = (bm -> buffertimer);
        bm -> buffertimer = bm -> buffertimer + 1;
        return RC_OK;
    }
    return RC_STRATEGY_NOT_FOUND;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum){
    int pnum,fg = 0;


    ReplacementStrategy str = bm->strategy;
    int pageCount = bm->numPages;
    int i= 0;
    while (i < pageCount)
    {

        if ((bm->mgmtData + i)->pageNum == -1)
        {
            void *ptr =calloc(PAGE_SIZE, sizeof(char));

            (bm->mgmtData + i)->data = (char*)ptr;
            fg = 1;
            pnum = i;
            break;
        }
        else if ((bm->mgmtData + i)->pageNum == pageNum)
        {
            fg = 2;
            pnum = i;
            if (bm->strategy == RS_LRU)
                AttributeUpdate(bm, bm->mgmtData + pnum);
            break;
        }
        else if (i == pageCount - 1)
        {

            fg = 1;
            if (str== RS_LRU)
            {
                pnum = strategyForFIFOandLRU(bm);
                if ((bm->mgmtData + pnum)->dirty)
                    forcePage (bm, bm->mgmtData + pnum);
            }
            else if (str== RS_FIFO)
            {
                pnum = strategyForFIFOandLRU(bm);
                if ((bm->mgmtData + pnum)->dirty)
                    forcePage (bm, bm->mgmtData + pnum);
            }

        }
        i++;
    }

    switch(fg)
    {
        case 1:
        {
            FILE *file = NULL;
            file = fopen(bm->pageFile, "r");
            long  size = pageNum * PAGE_SIZE;
//            char *data1 = (bm->mgmtData + pnum)->data;
            int file_offset = SEEK_SET;
            int x = fseek(file, size, file_offset);
            void * bmgmtData = (bm->mgmtData + pnum)->data;
            fread(bmgmtData, sizeof(char), PAGE_SIZE, file);
            page->data = (char*) bmgmtData;
            bm->numberReadIO = bm->numberReadIO + 1;
            (bm->mgmtData + pnum)->fixCounts = (bm->mgmtData + pnum)->fixCounts + 1;
            (bm->mgmtData + pnum)->pageNum = pageNum;
            int no_of_counts = (bm->mgmtData + pnum)->fixCounts;
            page->fixCounts = no_of_counts;
            bool dirty_val = (bm->mgmtData + pnum)->dirty;
            page->dirty = dirty_val;
            int pag_num = pageNum;
            page->pageNum = pag_num;
            int * strat_attribute = (bm->mgmtData + pnum)->strategyAttribute;
            page->strategyAttribute = strat_attribute;
            AttributeUpdate(bm, bm->mgmtData + pnum);
            fclose(file);
            break;
        }

        case 2:
        {

            int pag_num = pageNum;
            bool dirty_val = (bm->mgmtData + pnum)->dirty;
            void * bmgmtData = (bm->mgmtData + pnum)->data;
            int * strat_attribute = (bm->mgmtData + pnum)->strategyAttribute;
            int no_of_counts = (bm->mgmtData + pnum)->fixCounts;
            page->data = (char*) bmgmtData;
            (bm->mgmtData + pnum)->fixCounts++;
            page->fixCounts = no_of_counts;
            page->dirty = dirty_val;
            page->pageNum = pag_num;
            page->strategyAttribute =  strat_attribute;
            break;
        }
    }
    return RC_OK;
}





int getNumReadIO (BM_BufferPool *const bm) 
{
    int read = bm->numberReadIO;
    return read;
}

int getNumWriteIO (BM_BufferPool *const bm) 
{
    int write = bm->numberWriteIO;
    return write;
}


int *getAttributionArray(BM_BufferPool *bm)
{
    int *flg;


    void *x = calloc(bm->numPages, sizeof((bm->mgmtData)->strategyAttribute));

    int *atrbts = (int*) x;
    for (int i = 0;i < bm->numPages;i++)
    {
        flg = atrbts + i;
        *flg = *((bm->mgmtData + i)->strategyAttribute);

    }
    return atrbts;
}

int strategyForFIFOandLRU(BM_BufferPool *bm) 
{
    int minimum, abrtPg, i=0;



    int *fixCnt = (int*)malloc(bm->numPages * sizeof(int));
    while (i < bm->numPages)
    {
        fixCnt[i] = (bm->mgmtData + i) -> fixCounts;
        i++;
    }

    int num_pages = bm->numPages;
    minimum = bm->buffertimer;
    abrtPg = -1;

    for (i = 0; i < num_pages; ++i)
    {
        if (*(fixCnt + i) != 0)
            continue;

        else if (minimum >= (*(getAttributionArray(bm) + i)))
        {
            abrtPg = i;
            minimum = (*(getAttributionArray(bm) + i));
        }
    }

    if((minimum) < 32000){
        return abrtPg;

}

   else if ((bm->buffertimer) > 32000)
    {
        bm->buffertimer = bm->buffertimer - minimum;
        i = 0;
        while (i < bm->numPages) 
        {
            *(bm->mgmtData->strategyAttribute) = *(bm->mgmtData->strategyAttribute) - minimum;
            i++;
        }
    }
    return abrtPg;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int i = 0;
    while (i < bm->numPages)
    {
        if ((bm->mgmtData + i) -> pageNum == page -> pageNum)
        {
            (bm->mgmtData + i)-> fixCounts = (bm->mgmtData + i)-> fixCounts - 1;
            break;
        }
        i++;
    }
    return RC_OK;
}

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

