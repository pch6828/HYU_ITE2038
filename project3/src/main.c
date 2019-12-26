#include "bpt.h"
#include "file_manager.h"
#include "buffer_manager.h"
#include <string.h>
#include <inttypes.h>

// MAIN
int suc[11][3], fail[11][3];
int sum[3];
int main() {
	char cmd[10], value[120];
	int64_t key;
	int tid;// suc = 0, fail = 0, nomatch = 0,
	 int cnt=0;
	
	while (true) {
		scanf("%s", cmd);
		cnt++;
		if(!(cnt%10000)){
			printf("***%d***\n", cnt);
		}
		if (!strcmp(cmd, "0")) {
			scanf("%s", value);
			int id = open_table(value);
			printf("table id : %d\n", id);
		}
		else if (!strcmp(cmd, "1")) {
			scanf("%d", &tid);
			scanf("%"PRId64 "%s", &key, value);
			if(db_insert(tid, key, value)){
				fail[tid][0]++;
				//printf("INSERT %10"PRId64 "\t: FAIL\n", key);
			}
			else {
				suc[tid][0]++;
				//printf("INSERT %10"PRId64 "\t: SUCCESS\n", key);
			}
		}
		else if (!strcmp(cmd, "quit")) {
			shutdown_db();
			for(int i = 1; i <=10; i++){
				printf("tid : %d\n", i);
				for(int j = 0; j < 3; j++){
					printf("<%d> success : %d fail : %d\n",j ,suc[i][j], fail[i][j]);
					sum[j]+=suc[i][j];
				}
			}
			for(int i = 0; i < 3; i++){
				printf("%d : %d\n",i, sum[i]);
			}
			return 0;
		}else if(!strcmp(cmd, "print")){
			scanf("%d", &tid);
			print_tree(tid);
		}else if(!strcmp(cmd, "3")){
			scanf("%d", &tid);
			scanf("%"PRId64, &key);
			if(db_delete(tid, key)){
				fail[tid][1]++;
				//printf("DELETE %10"PRId64 "\t: FAIL\n", key);
			}
			else {
				suc[tid][1]++;
				//printf("DELETE %10"PRId64 "\t: SUCCESS\n", key);
			}
		}else if(!strcmp(cmd, "2")){
			scanf("%d", &tid);
			scanf("%"PRId64, &key);
			if(!db_find(tid, key, value)){
				if (key == atoi(value+4)) {
					//printf("SEARCH %10"PRId64 "\t: SUCCESS\n", key);
					suc[tid][2]++;
				}
				else {
					//printf("SEARCH %10"PRId64 "\t: ERROR\n", key);
				}
			}else{
				//printf("SEARCH %10"PRId64 "\t: FAIL\n", key);
				fail[tid][2]++;
			}
		}else if(!strcmp(cmd, "leaf")){
			scanf("%d", &tid);
			print_leaves(tid);
		}else if(!strcmp(cmd, "4")){
			scanf("%d", &tid);
			close_table(tid);
		}else if(!strcmp(cmd, "init")){
			int size;
			scanf("%d", &size);
			init_db(size);
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
