Storage Manager:

-Manipulating the Page Files
1. initStorageManager: We are calling this method to initialize the Storage Manager.
2. createPageFile: This method helps to create a page file using the fopen function and returning error when it fails.
3. openPageFile: This method openPageFile helps to open the file which has been created recently. It is returning error RC_FILE_NOT_FOUND for the non-existing file.
4. closePageFile: In this method, we are closing the previously opened file. We are closing the file and resetting the pointer to NULL.
5. destroyPageFile: We are using this destroyPageFile function in order to remove the file using remove(). 

-Reading blocks from the disk
1. readBlock: At first we are opening the file and reading the pagNum'th block from it. Then we are storing the data into the memory which is pointed to by the SM_PageHandle memPage. 
              The method is returning RC_READ_NON_EXISTING_PAGE if the file has less pageNum pages. We are updating the current page position and incrementing it by 1. 
2. getblockPos: Helps to return the current page position. 
3. readFirstBlock: Using this method, we are reading the first block of data. From the file we are reading the data to memory(memPage). 
4. readPreviousBlock: The readPreviousBlock helps to read the contents from the previous page. The offset is set to -PAGE_SIZE in the lseek().
5. readCurrentBlock: This method helps to read the current page data and the offset is set to 0. 
6. readNextBlock: We are using this method to read the next block of data and the offset is set to PAGE_SIZE. 
7. readLastBlock: This method helps in reading the last page and in the lseek() function, the offset is -PAGE_SIZE and the parameter is set to SEEK_END. 

-Writing blocks to a page file
1. writeBlock: We are using this method to write contents to disk. If the page number is non negative we are returning RC_WRITE_FAILED.
2. writeCurrentBlock : This method writes the current page to the disk. IF write(open(fHandle -> fileName, O_RDWR), memPage, PAGE_SIZE) returns greater than 0 we return RC_OK 
			else we return FILE_NOT_FOUND.
3. appendEmptyBlock : This method appends the number of pages in the file by one.
4. ensureCapacity : This method ensures that if the file has greater than number of pages then we decrease the size of number of pages.

How to Run:
Step 1: Open Command Promt/PowerShell/Terminal.

Step 2: Navigate to the Assignment 1 directory.

Step 3: Type command make and hit enter. (Files are complied and ready to be executed)

step 4: Type command make run_test and hit enter.

Step 5: Type command make clean to delete old compiled tests.

Step 6: Type command ./test_assign1 if you are using a Windows machine