#include "mapper.h"

//initialize the struct root
intermediateDS *root =  NULL;

// combined value list corresponding to a word <1,1,1,1....>
valueList *createNewValueListNode(char *value){
	valueList *newNode = (valueList *)malloc (sizeof(valueList));
	strcpy(newNode -> value, value);
	newNode -> next = NULL;
	return newNode;
}

// insert new count to value list
valueList *insertNewValueToList(valueList *root, char *count){
	valueList *tempNode = root;
	if(root == NULL)
		return createNewValueListNode(count);
	while(tempNode -> next != NULL)
		tempNode = tempNode -> next;
	tempNode -> next = createNewValueListNode(count);
	return root;
}

// free value list
void freeValueList(valueList *root) {
	if(root == NULL) return;

	valueList *tempNode = root -> next;;
	while (tempNode != NULL){
		free(root);
		root = tempNode;
		tempNode = tempNode -> next;
	}
}

// create <word, value list>
intermediateDS *createNewInterDSNode(char *word, char *count){
	intermediateDS *newNode = (intermediateDS *)malloc (sizeof(intermediateDS));
	strcpy(newNode -> key, word);
	newNode -> value = NULL;
	newNode -> value = insertNewValueToList(newNode -> value, count);
	newNode -> next = NULL;
	return newNode;
}

// insert or update a <word, value> to intermediate DS
intermediateDS *insertPairToInterDS(intermediateDS *root, char *word, char *count){
	intermediateDS *tempNode = root;
	if(root == NULL)
		return createNewInterDSNode(word, count);
	while(tempNode -> next != NULL) {
		if(strcmp(tempNode -> key, word) == 0){
			tempNode -> value = insertNewValueToList(tempNode -> value, count);
			return root;
		}
		tempNode = tempNode -> next;

	}
	if(strcmp(tempNode -> key, word) == 0){
		tempNode -> value = insertNewValueToList(tempNode -> value, count);
	} else {
		tempNode -> next = createNewInterDSNode(word, count);
	}
	return root;
}

// free the DS after usage. Call this once you are done with the writing of DS into file
void freeInterDS(intermediateDS *root) {
	if(root == NULL) return;

	intermediateDS *tempNode = root -> next;;
	while (tempNode != NULL){
		freeValueList(root -> value);
		free(root);
		root = tempNode;
		tempNode = tempNode -> next;
	}
}

// emit the <key, value> into intermediate DS
void emit(char *key, char *value) {
	root = insertPairToInterDS(root, key, value);
}

// map function
void map(char *chunkData){
	int i = 0;
	char *buffer;
	while ((buffer = getWord(chunkData, &i)) != NULL){
		emit(buffer, "1");
	}
}

// write intermediate data to separate word.txt files
// Each file will have only one line : word 1 1 1 1 1 ...
void writeIntermediateDS() {
	if(root == NULL) return;

	intermediateDS *tempNode = root;

	char one[] = {' ','1','\0'};

	while(tempNode != NULL){
		char prefix[] = {'/','\0'};

		char suffix[] = {'.','t','x','t','\0'};

		//allocate space for a word that is to be pathfile name
		char *word = malloc(strlen(mapOutDir) + strlen(prefix) + strlen(tempNode->key) + strlen(suffix) + 1);

		strcpy(word,mapOutDir);
		strcat(word,prefix);
		strcat(word,tempNode->key);
		strcat(word,suffix);

		FILE* outfile = fopen(word, "w");
		fprintf(outfile, "%s", tempNode->key);

		while(tempNode->value->value != NULL) {
			fprintf(outfile, "%s", one);
			tempNode->value = tempNode->value->next;
		}
		tempNode = tempNode->next;
		fclose(outfile);
	}
}

int main(int argc, char *argv[]) {

	if (argc < 2) {
		printf("Less number of arguments.\n");
		printf("./mapper mapperID\n");
		exit(0);
	}
	// ###### DO NOT REMOVE ######
	mapperID = strtol(argv[1], NULL, 10);

	// ###### DO NOT REMOVE ######
	// create folder specifically for this mapper in output/MapOut
	// mapOutDir has the path to the folder where the outputs of
	// this mapper should be stored
	mapOutDir = createMapDir(mapperID);

	// ###### DO NOT REMOVE ######
	while(1) {
		// create an array of chunkSize=1024B and intialize all
		// elements with '\0'
		char chunkData[chunkSize + 1]; // +1 for '\0'
		memset(chunkData, '\0', chunkSize + 1);

		char *retChunk = getChunkData(mapperID);
		if(retChunk == NULL) {
			break;
		}

		strcpy(chunkData, retChunk);
		free(retChunk);

		map(chunkData);
	}

	// ###### DO NOT REMOVE ######
	writeIntermediateDS();

	return 0;
}
