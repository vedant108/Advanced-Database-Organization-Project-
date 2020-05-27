Assignment 02: 
Team members: 
Siddharth Mishra
Kanyakumari Kashyap
Vedant Suram 


The main aim of this assignment is to implement the buffer manager. The buffer manager helps to manage a buffer of blocks in the memory. It reads the data to a disk or flush it into the disk and it
also implements block replacement. This helps to make more space in the memory and thus make it easy to read new blocks from the disk. 

With default test cases:
For compilation: make 
For Running: make run

To revert:
make clean


Buffer Pool Functions: 
The page file is already present on the disk. These methods helps to create a buffer pool for this page file. The storage manager (already implemented in the 1st assignment) helps in performing the operations of page file on the disk. 

1. initBufferPool: The initBufferPool function helps to create a new buffer pool in the memory. The initBufferPool method consists of parameters such as- 
(a) pageFileName- Every page file has a name and it is defined by the pageFileName.
(b) numPages- The size of the buffer is defined by the numPages. 
(c) strategy- We have implemented few strategies like FIFO (First In First Out), LRU(Least Recently Used), LFU(Least Frequently Used), CLOCK in the buffer pool for page replacement. 
(d) stratData- We are using the stratData in case if we are replacing the page as mentioned in the previous strategy. 

2. shutdownBufferPool: The shutdownBufferPool method helps to free the space used in the memory. The forecFlushPool() method calls all the modified pages and writes it into the disk. The shutdownBufferPool method then destroys the buffer pool.  

3. forceFlushPool: All the modified pages will be written to the disk (If dirty = 1 then the pages have been modified). If dirty=1 then it means that the pages have been modified and if pin =0 then it assumes that the page frame is not used by any user. If these conditions satisfies, then this method writes the modified page frame into the disk. 
This method 


Page Management Functions: 
The page management functions helps to remove the pages from the disk and to load into the buffer pool. Other few funtions which are also implemented by this method is to mark the modified pages as dirty i.e (dirty=1) and force the page to write into the disk. 

4. markDirty: The makeDirty function helps in page modification and set it to 1 i.e dirty =1.

5. unpinpage: The unpinPage method depends on the pageNum. The pin of the page is decremented by 1 when the client does not use the page. 

6. pinPage: The pinPage method checks if there are any free/empty spaces available in the buffer pool. It reads the page number from the page file present in the disk. This method also helps in storing the page frame in the buffer pool. 
The page replacement strategy is called if the conditions fails and then it replaces the page in the buffer pool. It implements the page replacement method by an algorithm which helps to identify which page has to be replaced and which page is modified. 

7. forcePage: The specific page is found by using the pageNum and then the contents of the page is written on the disk using the Storage manager. The value of the pin is set to 0. 

Statistics Functions:
These are the functions which helps to find the statistical information of buffer pool. 

8. getFrameContents: The number of pages present in the Buffer Pool can be obtained by this method. 

9. getDirtyFlags: If the page is modified/dirty then it returns the value as TRUE otherwise return FALSE. 

10. getFixCounts: This method helps to get the number of clients who use the pages from the Buffer Pool. The value of pin=0 if no client use the page frame. 

11. getNumReadIO: With the help of this function, we can get the number of pages which are read from the disk. 

12. getNumWriteIO: This method helps to obtain the number of pages that are written into the disk. 


Page Replacement Algorithm: 
Few algorithms are used in order to replace the pages from the buffer pool. These algorithms helps to find which pages are supposed to be replaced from the buffer pool. 

FIFO: The First In First Out (FIFO) algorithm can be defined as the first page will be replaced at first and then priorities will be given to the others. 

LRU: This algorithm  helps to remove or delete the page frame that are not used for long time in the buffer pool. This algorithm works when it finds the count of not long used page. If the value of the count is low then it writes the contents of the page into the disk.  

LFU: The Least Frequently Used algorithm deletes the page frame which are not frequently used. 

CLOCK: It helps in keeping track of the latest added page frame in the Buffer Pool. The CLOCK records the counts of the latest added page. Through this algorithm, it helps to find the position of the clock. The page is replaced with a new page if it is not added at the end in the page frame. 


 