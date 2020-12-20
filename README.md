My implementation of MapReduce in C

## Background

MapReduce is a programming model and an associated implementation for processing and generating big data sets with a parallel, distributed algorithm on a cluster

My implementation of MapReduce takes in and reads a text file. It records each word used, the number
of times it appears, and outputs a file containing each word with it's associated
word count.

## How to Compile
1. Change the directory to the Template folder.
2. On the command line type "make"
3. Next on the command line type "./mapreduce <#x of mappers> <#y of reducers> test/T1/T1.txt"
4. Note that the output file Reduce_reducerID.txt will be in the output file.

## Logic and Output of Mapper, MapReduce, Reducer
1. The master process takes in a file.
2. It allocates the files' contents to x different mapper processes (where x is the # specified in the command line).
3. Each mapper is fed a continuous chunk of bytes. Then will create multiple text files,
   each containing one word and a 1 for each time the word occurs in the chunk of bytes.
4. The master allocates y different reducer processes (where y is the # specified in the command line).
5. Reducer is fed a continuous stream of files from the mapper process.
6. Reducer takes the file, separates the word and counts each instance of 1 from the file.
7. It combines like words and totals the sub count of the word for all of the mapper process files.
8. It produces a text file, Reduce_reducerID.txt, that contains all words from the input file along with their totals.

## Logic and Output of the Master process
WHAT EXACTLY YOUR PROGRAM DOES:
1. The master process takes in chunks of bytes from a text file via sendChunkData().
  sendChunkData parses the text into bytes of size 1024.

2. Master allocates these byte chunks to getChunkData() via a message queue,
then getChunkData() sends the received bytes to n different mapper processes which
parse words into single words with a list of occurrences.

3. Each mapper is fed a continuous chunk of bytes. Then will create multiple text files,
each containing one word and a 1 for each time the word occurs in the chunk of bytes.

4. When all mapper processes terminate, master calls the shuffle process, which
divides the word.txt files in output/MapOut/map_<mapperID> via message queue to
n number of reducers.

5. getInterData() is used by master to retrieve file names from the MapOut/map_*
files via message queue, and getInterData() then uses these filenames to set
an output parameter that reducer.c uses

6. Reducer takes the file<name>, separates the word and counts each instance of 1 from the file.

7. It combines like words and totals the sub count of the word for all of the mapper process files.

8. It creates the directory output/ReduceOut, and writes n different text files
to that directory that contain a (singularly documented) word and the associated
number of times it appears.
