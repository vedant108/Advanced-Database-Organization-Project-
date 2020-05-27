#include "dberror.h"
#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
//--------------------------------------------------------------------------------
extern void initStorageManager (void)
{
    printf("\nInitialising Storage Manager");
}
//--------------------------------------------------------------------------------
extern RC createPageFile (char *fileName)
{
    FILE *f;
    f=fopen(fileName,"w+");
    if (f < 0)
        return RC_WRITE_FAILED;                                                       // Return error for failure in READ Write

    size_t i;
    for (i = 0; i < PAGE_SIZE; i++)
    {
        //fwrite('\0' , 1 , sizeof(char) , f);
        fprintf(f, "%c", '\0');
    }
    fclose(f);
    return RC_OK;
}
//--------------------------------------------------------------------------------
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{

    if(access(fileName, F_OK)!=0)
    	return RC_FILE_NOT_FOUND;
    else
    {
        FILE *f = fopen(fileName,"rb+");
        struct stat s;
        stat(fileName, &s);
        int totalPages = (s.st_size-1)/PAGE_SIZE;

        fHandle->totalNumPages = totalPages+1;
        fHandle->fileName = fileName;
        fHandle->mgmtInfo = f;
        fHandle->curPagePos = 0;
        return RC_OK;
    }
}
//--------------------------------------------------------------------------------
extern RC closePageFile(SM_FileHandle *fHandle)
{
    if(!fHandle)
        return RC_FILE_HANDLE_NOT_INIT; 				                                                           // Closing a file which is not open
    // Reset the pointer to NULL
    return RC_OK;
}
//--------------------------------------------------------------------------------
extern RC destroyPageFile (char *fileName)
{

    if(remove(fileName) == 0)
        return RC_OK;
    else
    {
        int fileD = open(fileName, O_RDONLY);                                                                 //Open the file
        if (fileD < 0)
            close(fileD);
        return RC_FILE_NOT_FOUND;

    }
}
//--------------------------------------------------------------------------------
extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    FILE *f = fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
    int num_pages = fHandle->totalNumPages;

    if((f == NULL) || (pageNum > num_pages ))
        return RC_READ_NON_EXISTING_PAGE;
    fseek(f,pageNum * PAGE_SIZE, SEEK_SET);
    fread(memPage, PAGE_SIZE,1,f);
    fHandle->curPagePos = pageNum;
    return RC_OK;
}
//--------------------------------------------------------------------------------
extern int getBlockPos (SM_FileHandle *fHandle)
{   
    return fHandle->curPagePos;;
}
//--------------------------------------------------------------------------------
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{

    if (open(fHandle -> fileName, O_RDONLY) < 0)                             //opening the file in read only mode
    {
        return RC_FILE_NOT_FOUND;                          // we are returning error when the file is not able to open
    }

    else if (open(fHandle -> fileName, O_RDONLY) >= 0)                         //we are opening the file and reading it
    {

        if (lseek(open(fHandle->fileName, O_RDONLY), 0, SEEK_SET) < 0) {
            close(open(fHandle->fileName, O_RDONLY));
            return RC_READ_NON_EXISTING_PAGE;
        }
        else {
            read(open(fHandle->fileName, O_RDONLY), memPage, PAGE_SIZE);           //we are obtaining the current position of the page
            fHandle->curPagePos = fHandle->curPagePos++;              //we are incrementing the current page position and reading the next page
            close(open(fHandle->fileName, O_RDONLY));
            return RC_OK;                                   //we are closing the file and returning non-error message
        }

    }

}

//--------------------------------------------------------------------------------
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int pageNum = fHandle->totalNumPages, num_pages = fHandle->totalNumPages;
    FILE *f = fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
  
    if(f!=NULL )
    {
	    fseek(f,PAGE_SIZE*(pageNum-1), SEEK_SET);
	    fread(memPage, PAGE_SIZE,1,f); 
	    fHandle->curPagePos= num_pages;
	    return RC_OK;
    }
    else
        return RC_READ_NON_EXISTING_PAGE;
}
//--------------------------------------------------------------------------------
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

    int filed = open(fHandle->fileName, O_RDONLY);

    if (open(fHandle->fileName, O_RDONLY) <0){                                                                       // We are checking whether we are able to open the file and it is returning error if we not open
        return RC_FILE_NOT_FOUND;
    }

    int x = lseek(filed, 0, SEEK_CUR);

    if (lseek(filed, 0, SEEK_CUR) <= 0) {
        close(filed);
        return RC_READ_NON_EXISTING_PAGE;

    } else if (lseek(filed, 0, SEEK_CUR) > 0)  {
        read(filed, memPage,PAGE_SIZE);                                                  // We are reading the data from the fdesc to the memory(memPage)
        close(filed);

        // It is returning error while going to the current page
    }
    return RC_OK;
}
//--------------------------------------------------------------------------------
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

    if (open(fHandle->fileName, O_RDONLY) < 0)                                                                       // Check whether we can open the file
        return RC_FILE_NOT_FOUND;                                                                    // return error if we cannot open the file


    else if (fHandle -> curPagePos == 0) {

        close(open(fHandle->fileName, O_RDONLY));
        return RC_READ_NON_EXISTING_PAGE;
    }

    else if((lseek(open(fHandle->fileName, O_RDONLY), -(PAGE_SIZE), SEEK_CUR)) >= 0){
        read(open(fHandle->fileName, O_RDONLY), memPage, PAGE_SIZE);                                                   // Read the data from file_descriptor to memory(memPage)
        fHandle -> curPagePos = fHandle -> curPagePos - 1;
        close(open(fHandle->fileName, O_RDONLY));
        return RC_OK;
    }
    else if((lseek(open(fHandle->fileName, O_RDONLY), -(PAGE_SIZE), SEEK_CUR)) < 0){
        close(open(fHandle->fileName, O_RDONLY));
        return RC_READ_NON_EXISTING_PAGE;
    }
}
//--------------------------------------------------------------------------------
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int pageNum = fHandle->curPagePos, pos = fHandle->curPagePos, num_pages = fHandle->totalNumPages;
    FILE *f = fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
    
    if((f == NULL) || ((pos + 1) > num_pages ))
        return RC_READ_NON_EXISTING_PAGE;
    
    fseek(f,1, SEEK_CUR);
    fread(memPage, PAGE_SIZE,1,f); 
    fHandle->curPagePos = pageNum + 1;
    return RC_OK;
}
//--------------------------------------------------------------------------------
static RC writePage(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    FILE *f = fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
    if((f == NULL) || (pageNum < -1))
        return RC_WRITE_FAILED;
    
    if (pageNum > fHandle->totalNumPages)
        fHandle->totalNumPages = pageNum + 1;
    
    fseek(f, pageNum * PAGE_SIZE, 0);
    fwrite(memPage,PAGE_SIZE,1,f); 
    if (ferror(f) || feof(f)) 
        return RC_WRITE_FAILED;
    return RC_OK;
}
//--------------------------------------------------------------------------------
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return writePage(pageNum, fHandle, memPage);
}
//--------------------------------------------------------------------------------
 RC appendEmptyBlock (SM_FileHandle *fHandle)
 {
    FILE *f = fHandle->mgmtInfo;
    
    if(f != NULL )
    {
	    int i = 0;
	    while(i<PAGE_SIZE)
	    {
	        fprintf(f,"%c",'\0');
	        i++;
	    }  
	    fHandle->totalNumPages = fHandle->totalNumPages + 1;
	    return RC_OK;
    }
    else
        return RC_FILE_NOT_FOUND;
}
//--------------------------------------------------------------------------------
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int pageNum = (fHandle->curPagePos);
    return writePage(pageNum, fHandle, memPage);
}
//--------------------------------------------------------------------------------
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
    if (fHandle->totalNumPages < numberOfPages)
    {
        int add_page = (numberOfPages - fHandle->totalNumPages)*PAGE_SIZE, i=0;
        while(i<add_page)
        {
            fprintf(fHandle->mgmtInfo, "%c",'\0');
            i++;
        }
        fHandle->totalNumPages = numberOfPages;
        return RC_OK;
    } 
    else 
        return RC_ENOUGH_NUMB_PAGES;
}
//--------------------------------------------------------------------------------