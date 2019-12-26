#include "bpt.h"
#include "file_manager.h"
#include "buffer_manager.h"
#include "join.h"
#include "trx_api.h"
#include "lock_manager.h"
#include "log_manager.h"
#include <iostream>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include <time.h>

using namespace std;

// MAIN

int table_id = 1;

pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;
//update onty
void* trx1(void* arg){
	int trx_id = begin_trx();
	//cout<<"TYPE<1>\t ID : "<<trx_id<<"\n";
	int64_t key;
	int query;
	key = 64;
	query = db_update(table_id, key, const_cast<char*>("aaaaa"), trx_id);
	//cout<<"TYPE<1>\t ID : "<<trx_id<<" UPDATE :"<<key<<" - "<<query<<"\n";
	key = 81;
	query = db_update(table_id, key, const_cast<char*>("bbbbb"), trx_id);
	//cout<<"TYPE<1>\t ID : "<<trx_id<<" UPDATE :"<<key<<" - "<<query<<"\n";
	key = 1097;
	query = db_update(table_id, key, const_cast<char*>("ccccc"), trx_id);
	//cout<<"TYPE<1>\t ID : "<<trx_id<<" UPDATE :"<<key<<" - "<<query<<"\n";
	key = 635;
	query = db_update(table_id, key, const_cast<char*>("ddddd"), trx_id);
	//cout<<"TYPE<1>\t ID : "<<trx_id<<" UPDATE :"<<key<<" - "<<query<<"\n";
	//int result = abort_trx(trx_id);
	int result = end_trx(trx_id);
	//cout<<"END<1>\t ID : "<<trx_id<<" result : "<<result<<"\n";
	return NULL;
}

//read onty
void* trx2(void* arg){
	int trx_id = begin_trx();
	//cout<<"TYPE<2>\t ID : "<<trx_id<<"\n";
	
	int64_t key;
	int query;
	char val[120];
	key = 64;
	query = db_find(table_id, key, val, trx_id);
	//cout<<"TYPE<2>\t ID : "<<trx_id<<" SEARCH :"<<key<<" - "<<val<<"\n";
	key = 81;
	query = db_find(table_id, key, val, trx_id);
	//cout<<"TYPE<2>\t ID : "<<trx_id<<" SEARCH :"<<key<<" - "<<val<<"\n";
	key = 1097;
	query = db_find(table_id, key, val, trx_id);
	//cout<<"TYPE<2>\t ID : "<<trx_id<<" SEARCH :"<<key<<" - "<<val<<"\n";
	key = 635;
	query = db_find(table_id, key, val, trx_id);
	//cout<<"TYPE<2>\t ID : "<<trx_id<<" SEARCH :"<<key<<" - "<<val<<"\n";
	int result = end_trx(trx_id);
	//cout<<"END<2>\t ID : "<<trx_id<<" result : "<<result<<"\n";
	return NULL;
}

int main(){
	init_db(100000);
	open_table(const_cast<char*>("DATA1"));
	for(int i = 0; i < 1; i++){
		pthread_t tx1, tx2, tx3;
		pthread_create(&tx1, 0, trx2, NULL);
		pthread_create(&tx2, 0, trx1, NULL);
		pthread_create(&tx3, 0, trx2, NULL);
		void* temp;	
		pthread_join(tx1, &temp);
		pthread_join(tx2, &temp);
		pthread_join(tx3, &temp);
		if(!(i%1000)){
			cout<<"\t\t\t\t\t"<<i<<"\n";
		}
	}
	print_log();
	//shutdown_db();
}

/*
int main() {
	char cmd[10], value[120];
	int64_t key;
	int tid;
	int cnt=0;
	
	while (true) {
		scanf("%s", cmd);
		cnt++;
		if(!(cnt%100000)){
			printf("***%d***\n", cnt);
		}
		if (!strcmp(cmd, "0")) {
			scanf("%s", value);
			int id = open_table(value);
			printf("table id : %d\n", id);
		}
		else if (!strcmp(cmd, "1")) {
			scanf("%d", &tid);
			scanf("%" PRId64 "%s", &key, value);
			if(db_insert(tid, key, value)){
				fail[tid][0]++;
			}
			else {
				suc[tid][0]++;
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
			scanf("%" PRId64, &key);
			if(db_delete(tid, key)){
				fail[tid][1]++;
			}
			else {
				suc[tid][1]++;
			}
		}else if(!strcmp(cmd, "2")){
			scanf("%d", &tid);
			scanf("%" PRId64, &key);
			if(!db_find(tid, key, value)){
				if (key == atoi(value+6)) {
					suc[tid][2]++;
				}
			}else{
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
		}else if(!strcmp(cmd, "5")){
			int tid1, tid2;
			scanf("%d%d%s", &tid1, &tid2, value);
			if(!join_table(tid1, tid2, value)){
				printf("JOIN %d %d \t:SUCCESS\n", tid1, tid2);
			}else{
				printf("JOIN %d %d \t:FAIL\n", tid1, tid2);
			}
		}
		fflush(stdin);	
	}
}
*/
