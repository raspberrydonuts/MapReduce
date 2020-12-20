#include <reducer.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h> // For exit()

//initialize the struct root
finalKeyValueDS *tempNode = NULL;

// create a key value node
finalKeyValueDS *createFinalKeyValueNode(char *word, int count){
	finalKeyValueDS *newNode = (finalKeyValueDS *)malloc (sizeof(finalKeyValueDS));
	strcpy(newNode -> key, word);
	newNode -> value = count;
	newNode -> next = NULL;
	return newNode;
}

// insert or update an key value
finalKeyValueDS *insertNewKeyValue(finalKeyValueDS *root, char *word, int count){
	finalKeyValueDS *tempNode = root;
	if(root == NULL)
		return createFinalKeyValueNode(word, count);
	while(tempNode -> next != NULL){
		if(strcmp(tempNode -> key, word) == 0){
			tempNode -> value += count;

			return root;
		}
		tempNode = tempNode -> next;
	}
	if(strcmp(tempNode -> key, word) == 0){
		tempNode -> value += count;
	} else{
		tempNode -> next = createFinalKeyValueNode(word, count);
	}
	return root;
}

// free the DS after usage. Call this once you are done with the writing of DS into file
void freeFinalDS(finalKeyValueDS *root) {
	if(root == NULL) return;

	// finalKeyValueDS *tempNode = NULL;
	//   while (root != NULL)
	// 	tempNode = root;
	// 	root = root -> next;

	finalKeyValueDS *tempNode = root -> next;
			while (tempNode != NULL){
			//free(root);
			root = tempNode;
			tempNode = tempNode -> next;
			free(tempNode);
		}
}

// reduce function
void reduce(char *key) {

	//open file pointers
 	FILE *fp;

	char * pointer;

	//str holds the passed in key
    char str[500];
	char word[500];

	//for printing intermediate data
	int total = 0;
	int ones = 0;
	int num = 0;

	//opens the file for read
    fp = fopen(key,"r");

	if(fp == NULL){
		printf("Could not open file\n");
		return;
	}

	//read the file into the array str
	fread(str, sizeof(char), 100, fp);

	//puts the word into it's own array
	sscanf(str, "%s", word);

	//puts the length of only the word in n
    sscanf(str, "%*[^0-9]%n", &num);

	//pointer is the place where the word ends
	pointer = num + str;

	//ones is the, well, 1
	while( 1 == sscanf(pointer, "%d%n", &ones, &num)){
		++total;									//x is how many v are counted
		pointer += num;						//move the pointer
	}

	//sends the root,key, value to insertNewKeyValue
 	tempNode = insertNewKeyValue(tempNode, word, total);

	//close the file pointers
	fclose(fp);

	return;
	}


// write the contents of the final intermediate structure
// to output/ReduceOut/Reduce_reducerID.txt
void writeFinalDS(int reducerID){

//variables for output name and path
	char outName[50] = "output/ReduceOut/Reduce_reducerID.txt";
	char name1[50] = "output/ReduceOut/Reduce_";
	char name2[50];
	char name3[10] = ".txt";
    char fname[100];
	char word[50];
	char value[50];


//concatenate the strings for output
	strcat(fname,name1);
	sprintf(name2, "%d",reducerID);
	strcat(fname,name2);
	strcat(fname,name3);

//open file for append
	FILE *fp2;
	FILE *fp3;

	fp2 = fopen(fname,"a");
	fp3 = fopen(outName, "a");

	if(fp2 == NULL || fp3 == NULL){
		printf("Could not open file\n");
		return;
	}

//assign tempNode to another variable
	finalKeyValueDS *printNode;//tempNode;
	printNode = tempNode->next;

//put the keys and values into the file
	while (printNode != NULL){
		sprintf(word,"%s",printNode->key);
		sprintf(value," %d\n",printNode->value);
		fputs(word,fp2);
		fputs(value,fp2);
		fputs(word,fp3);
		fputs(value,fp3);
		printNode = printNode->next;
	}

	//close the file
	fclose(fp2);
	fclose(fp3);

	return;
}

int main(int argc, char *argv[]) {

	if(argc < 2){
		printf("Less number of arguments.\n");
		printf("./reducer reducerID");
	}

	// ###### DO NOT REMOVE ######
	// initialize
	int reducerID = strtol(argv[1], NULL, 10);

	// ###### DO NOT REMOVE ######
	// master will continuously send the word.txt files
	// alloted to the reducer

	char key[MAXKEYSZ];
	while(getInterData(key, reducerID))
		reduce(key);

	// You may write this logic. You can somehow store the
	// <key, value> count and write to Reduce_reducerID.txt file
	// So you may delete this function and add your logic
    writeFinalDS(reducerID);

	//free the database
	freeFinalDS(tempNode);

	return 0;
}
