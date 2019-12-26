#ifndef __LOCK_MANAGER_H__
#define __LOCK_MANAGER_H__

#include<list>
#include<vector>
#include<unordered_map>
#include<set>
#include<string.h>
#include<pthread.h>
#include"file_manager.h"
#include"buffer_manager.h"

//Transaction Serializer Implementation
#define SERIALIZER

#ifdef SERIALIZER
extern pthread_mutex_t action_mutex;
extern pthread_cond_t action_cond;
extern volatile int action_flag;
#endif

enum lock_mode{
	SHARED, 
	EXCLUSIVE
};

enum lock_state{
	LOCK_SUCCESS,
	LOCK_WAITING,
	LOCK_DEADLOCK
};

enum trx_state{
	IDLE,
	RUNNING,
	WAITING
};

class trx_t;
class lock_t;
class undo_log_t;

class lock_t{
	public:
		int table_id;
		pagenum_t page_no;
		lock_mode mode;
		lock_state state;
		trx_t* trx;
		undo_log_t* undo;
		lock_t(int table_id, pagenum_t pagenum, lock_mode mode, trx_t* trx, undo_log_t* undo){
			table_id = table_id;
			page_no = pagenum;
			mode = mode;
			trx = trx;
			undo = undo;
		}
};

class undo_log_t {
	public:
		int64_t key;
		char before[120];

		undo_log_t(int64_t key, char* value) {
			this->key = key;
			memcpy(before, value, 120);
		}
		
};

class trx_t{
	public:
		int trx_id;
		trx_state state;
		std::list<lock_t*>trx_locks;
		std::vector<lock_t*> wait_lock;
		pthread_cond_t trx_cond;
		trx_t(int trx_id){
			trx_id = trx_id;
			state = IDLE;
			trx_locks = std::list<lock_t*>();
			wait_lock = std::vector<lock_t*>();
			trx_cond = PTHREAD_COND_INITIALIZER;
		}
};

extern volatile int cnt;
extern std::unordered_map<std::pair<int, pagenum_t>, std::list<lock_t*>, Hash>lock_table;
extern pthread_mutex_t lock_mutex;

extern std::unordered_map<int, trx_t*>trx_table;
extern pthread_mutex_t trx_mutex;
extern std::set<int>aborted_trx;
//TRX API
int begin_trx();
int end_trx(int trx_id);
int abort_trx(int trx_id);

//LOCK API
lock_state acquire_lock(int table_id, pagenum_t pagenum, int trx_id, lock_mode mode, undo_log_t* undo);
void release_lock(int trx_id);
bool deadlock_detect(int trx_id, std::vector<int>&v);
#endif /*__LOCK_MANAGER_H__*/
