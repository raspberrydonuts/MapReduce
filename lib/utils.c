#include "utils.h"
#include "sys/msg.h"
#include "sys/stat.h"
#include "errno.h"

#define keyNum 5370869 // Jared's student ID

char *getChunkData(int mapperID) {
  // Open message queue
  struct msgBuffer msg;
  key_t key;
  int msgid;
  key = ftok(".", keyNum);
  if ((msgid = msgget(key, 0666 | IPC_CREAT)) == -1) {
    printf("Failed to open message queue\n");
    exit(0);
  }

  // Receive chunk from the master
  if ((msgrcv(msgid, (void *)&msg, sizeof(msg.msgText), mapperID, 0)) == -1) {
    printf("Failed to receive message\n");
    exit(0);
  }

  // Check for END message and send to master
  if (strcmp(msg.msgText, "END") == 0) {
    return NULL;
  }

  char * messageText = (char *) malloc(MSGSIZE * sizeof(char));
  memcpy(messageText, msg.msgText, MSGSIZE);
  return messageText;
}

// sends message to message queue using mapperId
void sendMessage(struct msgBuffer *message, int *mapperId, int nMappers, int msgid) {
	message->msgType = (*mapperId);

	// send message to mapper
	if (msgsnd(msgid, (void *)message, sizeof(message->msgText), 0) == -1) {
		printf("Failed to send message\n");
		exit(0);
	}

	// printf("Message type (Mapper Id) is: %ld\n", message->msgType);
	// printf("Message text sent is: %s\n", message->msgText);
	memset(message->msgText, '\0', MSGSIZE);

	// go back to first mapper if we've used all mappers (round robin)
	if ((*mapperId) == nMappers) {
		(*mapperId) = 0;
	}

	// increment for next mapper to use
	(*mapperId)++;
}

// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers) {
  // open file to get number of characters
  FILE *fp = fopen(inputFile, "r");
  if (fp == NULL) {
    printf("Failed to open file\n");
    exit(0);
  }

  // get the number characters of file
  // used later to know that we have read every byte
  int totalNumChars = 0;
  char c;
  for (c = getc(fp); c != EOF; c = getc(fp)) {
    totalNumChars = totalNumChars + 1;
  }

  fclose(fp);

  if (totalNumChars == 0) {
    printf("Empty file\n");
    exit(0);
  }

  // open file to read the content
  fp = fopen(inputFile, "r");
  if (fp == NULL) {
    printf("Failed to open file\n");
    exit(0);
  }

  // open message queue
  key_t key;
  int msgid;
  key = ftok(".", keyNum); 

  if ((msgid = msgget(key, 0666 | IPC_CREAT)) == -1) {
    printf("Failed to open message queue\n");
    exit(0);
  }
  // check if msgqueue exists already
  if (msgctl(msgid, IPC_RMID, NULL) < 0) {
    printf("Failed to check for previous message queues\n");
  }

  if ((msgid = msgget(key, 0666 | IPC_CREAT)) == -1) {
    printf("Failed to open message queue\n");
    exit(0);
  }

  // initialize variables for constructing chunks
  struct msgBuffer message;
  int bytesRead = 0;
  int totalBytesRead = 0;
  int mapperId = 1;
  char ch;
  char word[100]; // max word size is 100 chars
  int wordLength = 0;
  memset(message.msgText, '\0', MSGSIZE);

  // construct chunks of at most 1024 bytes and send to mapper in RR fashion

  while ((ch = fgetc(fp)) != EOF) {
    // if not a valid character, concat to msgText
    if (!validChar(ch) || ch == ' ' || ch == '\n') {
      strncat(message.msgText, &ch, 1);
      bytesRead++;
      totalBytesRead++;

      // if the invalid character perfectly fits chunk, send it
      if (bytesRead == chunkSize) {
        sendMessage(&message, &mapperId, nMappers, msgid);
        bytesRead = 0;
      }

    } else {  // otherwise if character is a valid character
      wordLength = 0;
      memset(word, '\0', sizeof(word));

      // collect next few chars as a word
      while (validChar(ch)) {
        strncat(word, &ch, 1);
        ch = fgetc(fp);
        if (!validChar(ch) || ch == ' ' || ch == '\n') {
          fseek(fp, -1, SEEK_CUR);
        }
        wordLength++;
      }
      // if the word fits into the chunk, concat it to msgText
      if ((bytesRead + wordLength < chunkSize)) {
        strncat(message.msgText, word, wordLength);
        bytesRead = bytesRead + wordLength;
        totalBytesRead = totalBytesRead + wordLength;
      } else if ((bytesRead + wordLength == chunkSize)) {
      // else if the word fits into chunk perfectly, concat and send it

	  strncat(message.msgText, word, wordLength);
	  sendMessage(&message, &mapperId, nMappers, msgid);
	  bytesRead = 0;
	  totalBytesRead = totalBytesRead + wordLength;
      } else {
      // otherwise if whole word can't fit in chunk, send what we have
      // and revert our position in the file

	  fseek(fp, -wordLength, SEEK_CUR);
	  sendMessage(&message, &mapperId, nMappers, msgid);
	  bytesRead = 0;
      }
    }
    // finally, send what we have if we're done reading the file
    if (totalBytesRead == totalNumChars) {
      sendMessage(&message, &mapperId, nMappers, msgid);
    }
  }
  // send END message to mappers
  memset(message.msgText, '\0', MSGSIZE);
  sprintf(message.msgText, "END");
  for (int j = 1; j <= nMappers; j++) {
    message.msgType = j;
    if (msgsnd(msgid, (void *)&message, sizeof(message.msgText), 0) == -1) {
      printf("Failed to send END message to mapper\n");
      exit(0);
    }
  }
  fclose(fp);
}

// hash function to divide the list of word.txt files across reducers
// http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
  unsigned long hash = 0;
  int c;

  while ((c = *key++)!='\0')
    hash = c + (hash << 6) + (hash << 16) - hash;

  return (hash % reducers);
}



int getInterData(char *wordFileName, int reducerID) {

	key_t qkey;
	qkey = ftok(".", keyNum);
	int msgid;
	struct msgBuffer msg;
	msg.msgType = reducerID;

	// attempt to open a handle to the message queue, return error if unsuccessful:
	if((msgid = msgget(qkey, 0666 | IPC_CREAT)) < 0) {
		printf("Error in getting ahold of the message queue from within getInterData(). \n");
		exit(0);
	}

	char chk[] = {'\0','\0','\0','\0'};

	// msgrcv() returns the number of bytes in the message if successful,
	// as well as the word file name, otherwise it returns -1 and sets errno:
	if(msgrcv(msgid, (void *)&msg, sizeof(msg.msgText), reducerID, 0) < 0) {
		printf("Unable to retrieve file name from message queue within getInterData()\n");
		exit(0);
	}

	// set up chk variable to look at the first three characters recieved and
	// see if it is the "END" message
	strncpy(chk, msg.msgText, 3);

	// if END signal recieved, return 0 to return "not true" for while loop
	// back in reducer.c:
	if(strncmp(chk, "END", 3) == 0) {
		return 0;
	}
	else { // set output parameter wordFileName to the recieved file name
		// memory allocate newWordFileName & filled with null chars:
		char newWordFileName[50];
		memset(newWordFileName, '\0', 50);
		strcpy(newWordFileName, msg.msgText);  // copy in pathname (it is given that pathname is not > 50)
		strcpy(wordFileName, newWordFileName);  // copy pathname into the output param
	}
	return 1;
}


void shuffle(int nMappers, int nReducers) {  
  // open message queue
  //messageQueue = openMessageQueue();
  int reducerID;
  key_t key;
  int msgid;
  struct msgBuffer msg;
  char *wordFileName;

  key = ftok(".", keyNum);

  if ((msgid = msgget(key, 0666 | IPC_CREAT)) == -1) {
    printf("Failed to open message queue\n");
    exit(0);
  }
  // see if msg queue existing, then call CTRL directly. ALso do this in shuffle
  if (msgctl(msgid, IPC_RMID, NULL) < 0) {
    printf("Failed to check for previous message queues\n");
  }

  if ((msgid = msgget(key, 0666 | IPC_CREAT)) == -1) {
    printf("Failed to open message queue\n");
    exit(0);
  }

  struct dirent *dr;
  // DIR *dp;
  // dp = opendir("output/MapOut");



  /* traverse the directory of each Mapper and send the word filepath to the reducers
  for each mapper do
  for each wordFileName in mapOutDir do */
  char dirName[50];
  for (int i = 1; i <= nMappers; i++) {
    char mapperFolder[12];
    strcpy(dirName, "output/MapOut/");
    sprintf(mapperFolder, "Map_%d", i);
    strcat(dirName, mapperFolder);

    DIR * dp = opendir(dirName);
    if (dp == NULL) {
      printf("Error in opening the output directory.\n");
      exit(0);
    }

    while (((dr = readdir(dp)) != NULL)) {

      if(strcmp(dr->d_name, ".") == 0 || strcmp(dr->d_name, "..") == 0) {
      // skip . and .. dirs
      } else {
        char fileName[50];
	strcpy(fileName, dirName);
	strcat(fileName, "/");

	strcat(fileName, dr->d_name);
	memset(msg.msgText, '\0', MSGSIZE);
	// sprintf(msg.msgText, "%s", fileName);
	strcpy(msg.msgText, fileName);

	/*  ^^^^ select the reducer using a hash function
	reducerId = hashFunction(wordFileName,nReducers)∗; */

	reducerID = hashFunction(dr->d_name, nReducers) + 1; //+1 since minumum is 0 for hashFunction
	msg.msgType = reducerID;
	/* send word filepath to reducer
	messageSend(messageQueue, wordF ileP ath, reducerId);
	end while loop
	end for loop */

	//messageSend(msgid, "/output/MapOut/Map_%d", mapperID, reducerID);
	if (msgsnd(msgid, (void *)&msg, sizeof(msg.msgText), 0) == -1) {
	  printf("Failed to send message.\n");
	  exit(0);
	}
      }
    } //end while
  closedir(dp);
  } //end for

  // send END message to reducers
  memset(msg.msgText, '\0', MSGSIZE);
  sprintf(msg.msgText, "END");
  for (int j = 1; j <= nReducers; j++) {
    msg.msgType = j;
    if (msgsnd(msgid, (void *)&msg, sizeof(msg.msgText), 0) == -1) {
      printf("Failed to send END message to reducers. \n");
      exit(0);
    }
  }
}

// check if the character is valid for a word
int validChar(char c){
  return (tolower(c) >= 'a' && tolower(c) <='z') || (c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
  char *buffer = (char *)malloc(sizeof(char) * chunkSize);
  memset(buffer, '\0', chunkSize);
  int j = 0;
  while((*i) < strlen(chunk)) {
  // read a single word at a time from chunk
    if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
      buffer[j] = '\0';
      if(strlen(buffer) > 0){
	(*i)++;
	return buffer;
      }
      j = 0;
      (*i)++;
      continue;
    }
    buffer[j] = chunk[(*i)];
    j++;
    (*i)++;
    }
    if(strlen(buffer) > 0)
      return buffer;
  return NULL;
}

void createOutputDir(){
  mkdir("output", ACCESSPERMS);
  mkdir("output/MapOut", ACCESSPERMS);
  mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
  char *dirName = (char *) malloc(sizeof(char) * 100);
  memset(dirName, '\0', 100);
  sprintf(dirName, "output/MapOut/Map_%d", mapperID);
  mkdir(dirName, ACCESSPERMS);
  return dirName;
}

void removeOutputDir(){
  pid_t pid = fork();
  if(pid == 0){
    char *argv[] = {"rm", "-rf", "output", NULL};
    if (execvp(*argv, argv) < 0) {
      printf("ERROR: exec failed\n");
      exit(1);
    }
    exit(0);
  } else {
    wait(NULL);
  }
}

void bookeepingCode(){
  removeOutputDir();
  sleep(1);
  createOutputDir();
}
