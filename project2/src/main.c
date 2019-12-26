#include "bpt.h"
#include "file_manager.h"
#include <string.h>
#include <inttypes.h>

// MAIN

int main() {
	char cmd[10], value[120];
	int64_t key;

	while (true) {
		scanf("%s", cmd);
		if (!strcmp(cmd, "open")) {
			scanf("%s", value);
			open_table(value);
		}
		else if (!strcmp(cmd, "insert")) {
			scanf("%"PRId64 "%s", &key, value);
			if(db_insert(key, value)){
				printf("INSERT %10"PRId64 "\t: FAIL\n", key);
			}
			else {
				printf("INSERT %10"PRId64 "\t: SUCCESS\n", key);
			}
		}
		else if (!strcmp(cmd, "quit")) {
			return 0;
		}else if(!strcmp(cmd, "print")){
			print_tree();
		}else if(!strcmp(cmd, "delete")){
			scanf("%"PRId64, &key);
			if(db_delete(key)){
				printf("DELETE %10"PRId64 "\t: FAIL\n", key);
			}
			else {
				printf("DELETE %10"PRId64 "\t: SUCCESS\n", key);
			}
		}else if(!strcmp(cmd, "find")){
			scanf("%"PRId64, &key);
			if(!db_find(key, value)){
				if (key == atoi(value)) {
					printf("SEARCH %10"PRId64 "\t: SUCCESS\n", key);
				}
				else {
					printf("SEARCH %10"PRId64 "\t: ERROR\n", key);
				}
			}else{
				printf("SEARCH %10"PRId64 "\t: FAIL\n", key);
			}
		}else if(!strcmp(cmd, "leaf")){
			print_leaves();
		}	
	}
}

/*
int main(){
	char cmd[10], value[120];
        int64_t key;
	scanf("%s", cmd);
        if (!strcmp(cmd, "open")) {
                scanf("%s", value);
                open_table(value);
        }
	for(int64_t i = 0; i < 4000; i++){
	//	db_insert(i, "a");
	}
	print_tree();

}
*/
