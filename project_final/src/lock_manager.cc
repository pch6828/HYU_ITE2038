#include<algorithm>
#include<list>
#include<unordered_map>
#include<set>
#include"lock_manager.h"
#include"log_manager.h"

using namespace std;

volatile int cnt = 0;
unordered_map<pair<int, pagenum_t>, list<lock_t*>, Hash>lock_table;
unordered_map<int, trx_t*>trx_table;
set<int>aborted_trx;

pthread_mutex_t lock_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t trx_mutex = PTHREAD_MUTEX_INITIALIZER;

#ifdef SERIALIZER
pthread_mutex_t action_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t action_cond = PTHREAD_COND_INITIALIZER;
volatile int action_flag = 0;
#endif

//TRX API
int begin_trx(){
#ifndef SERIALIZER
	pthread_mutex_lock(&trx_mutex);
	int trx_id = ++cnt;
	
	trx_table[trx_id] = new trx_t(trx_id);
	pthread_mutex_unlock(&trx_mutex);
	return trx_id;
#endif

#ifdef SERIALIZER
	pthread_mutex_lock(&action_mutex);
	while (action_flag) {
		pthread_cond_wait(&action_cond, &action_mutex);
	}
	action_flag = 1;
	int trx_id = ++cnt;
	make_new_log(trx_id, BEGIN);
	pthread_mutex_unlock(&action_mutex);
	return trx_id;
#endif

}

int end_trx(int trx_id){
#ifndef SERIALIZER
	pthread_mutex_lock(&trx_mutex);
	if (aborted_trx.count(trx_id)) {
		aborted_trx.erase(trx_id);
		delete trx_table[trx_id];
		trx_table.erase(trx_id);
		pthread_mutex_unlock(&trx_mutex);
		return 0;
	}

	release_lock(trx_id);
	delete trx_table[trx_id];
	trx_table.erase(trx_id);
	
	pthread_mutex_unlock(&trx_mutex);
	return trx_id;
#endif

#ifdef SERIALIZER
	action_flag = 0;
	make_new_log(trx_id, COMMIT);
	flush_log();	
	pthread_cond_signal(&action_cond);
	return 0;
#endif
}

int abort_trx(int trx_id) {
	make_new_log(trx_id, ABORT);
	rollback(trx_id);
	return end_trx(trx_id);
};

//LOCK API
lock_state acquire_lock(int table_id, pagenum_t pagenum, int trx_id, lock_mode mode, undo_log_t* undo) {
	pthread_mutex_lock(&lock_mutex);

	auto lock_list = lock_table.find({ table_id, pagenum });

	if (lock_list == lock_table.end() || lock_list->second.empty()) {
		lock_t* lock = new lock_t(table_id, pagenum, mode, trx_table[trx_id], undo);
		lock->state = LOCK_SUCCESS;
		lock_table[{table_id, pagenum}].push_back(lock);
		pthread_mutex_unlock(&lock_mutex);
		return LOCK_SUCCESS;
	}
	else {
		bool able = 1;
		for (auto it = lock_list->second.begin(); it != lock_list->second.end(); it++) {
			if ((*it)->table_id == table_id && (*it)->page_no == pagenum) {
				if ((*it)->trx->trx_id == trx_id) {
					if((*it)->mode==mode){
						pthread_mutex_unlock(&lock_mutex);
						return (*it)->state;
					}else if((*it)->mode==SHARED){
						(*it)->mode=EXCLUSIVE;
						pthread_mutex_unlock(&lock_mutex);
						return (*it)->state;
					}
				}
				else if ((*it)->trx->trx_id != trx_id) {
					able = ((*it)->mode == SHARED && mode == SHARED);
				}
			}
		}

		if (able) {
			lock_t* lock = new lock_t(table_id, pagenum, mode, trx_table[trx_id], undo);
			lock->state = LOCK_SUCCESS;
			lock_list->second.push_back(lock);
			pthread_mutex_unlock(&lock_mutex);
			return LOCK_SUCCESS;
		}
		else {
			vector<int>v;
			for (auto it = lock_list->second.begin(); it != lock_list->second.end(); it++) {
				if ((*it)->trx->trx_id != trx_id) {
					v.push_back((*it)->trx->trx_id);
				}
			}
			if (deadlock_detect(trx_id, v)) {
				pthread_mutex_unlock(&lock_mutex);
				return LOCK_DEADLOCK;
			}
			else {
				lock_t* lock = new lock_t(table_id, pagenum, mode, trx_table[trx_id], undo);
				lock->state = LOCK_WAITING;
				pthread_mutex_lock(&trx_mutex);
				auto trx = trx_table[trx_id];
				trx->state = WAITING;
				for (auto it = lock_list->second.begin(); it != lock_list->second.end(); it++) {
					if ((*it)->table_id == table_id && (*it)->page_no == pagenum && (*it)->trx->trx_id != trx_id) {
						if (find(trx->wait_lock.begin(), trx->wait_lock.end(), *it)==trx->wait_lock.end()) {
							trx->wait_lock.push_back(*it);
						}

					}
				}
				pthread_mutex_unlock(&trx_mutex);
				lock_list->second.push_back(lock);
				pthread_mutex_unlock(&lock_mutex);
				return LOCK_WAITING;
			}
		}
	}
}

void release_lock(int trx_id) {
	pthread_mutex_lock(&lock_mutex);

	auto trx = trx_table[trx_id];
	
	for (auto it = trx->trx_locks.begin(); it != trx->trx_locks.end(); it++) {
		auto& lock_list = lock_table[{(*it)->table_id, (*it)->page_no}];
		auto i = lock_list.begin();
		while (i != lock_list.end()) {
			if ((*i)->trx->trx_id == trx_id) {
				delete (*i);
				i = lock_list.erase(i);
			}
			else {
				i++;
			}
		}
			
		i = lock_list.begin();
		if (i == lock_list.end()) {
			continue;
		}
		int first_trx = (*i)->trx->trx_id;
		while (i != lock_list.end()) {
			if ((*i)->trx->trx_id == first_trx) {				
				(*i)->state = LOCK_SUCCESS;
				auto trx = trx_table[first_trx];
				trx->state = RUNNING;
				pthread_cond_signal(&trx->trx_cond);
			}
			i++;
		}
	}

	pthread_mutex_unlock(&lock_mutex);
};

void dfs(int now, vector<int>* adj, int* visit, bool* cycle) {
	visit[now] = 1;
	for (auto nxt : adj[now]) {
		if (visit[nxt] == 0) {
			dfs(nxt, adj, visit, cycle);
		}
		else if (visit[nxt] == 1) {
			*cycle = true;
		}
	}
	visit[now] = 2;
}
bool deadlock_detect(int trx_id, vector<int>& v) {
	pthread_mutex_lock(&trx_mutex);
	vector<int>* adj = new vector<int>[cnt + 1];
	vector<int>trx;
	for (int i : v) {
		adj[trx_id].push_back(i);
	}

	for (auto p : trx_table) {
		trx.push_back(p.first);
		for (auto w : p.second->wait_lock) {
			adj[p.first].push_back(w->trx->trx_id);
		}
	}
	int* visit = new int[cnt + 1];
	for (int i = 0; i < cnt + 1; i++) {
		visit[i] = 0;
	}
	bool cycle = false;

	for (auto t : trx) {
		dfs(t, adj, visit, &cycle);
		if (cycle) {
			pthread_mutex_unlock(&trx_mutex);
			return true;
		}
	}			
	pthread_mutex_unlock(&trx_mutex);
	return false;
}
