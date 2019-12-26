#include"lock_manager.h"
#include"trx_api.h"
#include"file_manager.h"
#include"bpt.h"
#include"log_manager.h"
#include"buffer_manager.h"

using namespace std;

int db_find(int table_id, int64_t key, char* value, int trx_id){
#ifndef SERIALIZER
	pthread_mutex_lock(&trx_mutex);
	if(aborted_trx.count(trx_id)){
		pthread_mutex_unlock(&trx_mutex);
		return ABORT;
	}
	pthread_mutex_unlock(&trx_mutex);
	int idx;
	while(true){
		pthread_mutex_lock(&buffer_mutex);
		pagenum_t page_no = find_leaf(table_id, key);
		if(!page_no){
			pthread_mutex_unlock(&buffer_mutex);
			return FAIL;
		}

		idx = buf_get_page(table_id, page_no);
		pthread_mutex_lock(&(buffer->frames[idx].page_mutex));
		pthread_mutex_unlock(&buffer_mutex);

		lock_state state = acquire_lock(table_id, page_no, trx_id, SHARED, NULL);

		if(state == LOCK_SUCCESS){
			break;
		}else if(state == LOCK_WAITING){	
			pthread_mutex_unlock(&(buffer->frames[idx].page_mutex));
			pthread_mutex_lock(&trx_mutex);
			auto trx = trx_table[trx_id];
			trx->state = WAITING;
			pthread_cond_wait(&(trx->trx_cond), &trx_mutex);
			pthread_mutex_unlock(&trx_mutex);
		}else{
			pthread_mutex_unlock(&(buffer->frames[idx].page_mutex));
			abort_trx(trx_id);
			return ABORT;
		}
	}
	page_t* page = buffer->frames[idx].page;
	buffer->frames[idx].is_pinned++;
	for(int i = 0; i < page->header.num_keys; i++){
		if(key==page->records[i].key){
			memcpy(value, page->records[i].value, 120);
			buffer->frames[idx].is_pinned--;
			pthread_mutex_unlock(&(buffer->frames[idx].page_mutex));
			return SUCCESS;
		}
	}

	buffer->frames[idx].is_pinned--;
	pthread_mutex_unlock(&(buffer->frames[idx].page_mutex));
	
	return FAIL;
#endif

#ifdef SERIALIZER
	if (!buffer_allocated) {
		return 1;
}
	if (table_id <= 0 || table_id > 10) {
		return 1;
	}
	if (!id_table || id_table->id[table_id] == -1) {
		return 1;
	}
	int i = 0;
	pagenum_t pagenum = find_leaf(table_id, key);
	page_t* n;
	if (!pagenum) {
		return 1;
	}
	int now_idx = buf_get_page(table_id, pagenum);
	buffer->frames[now_idx].is_pinned++;
	n = buffer->frames[now_idx].page;
	for (i = 0; i < n->header.num_keys; i++) {
		if (n->records[i].key == key) {
			memcpy(value, n->records[i].value, 120);
			buffer->frames[now_idx].is_pinned--;
			return 0;
		}
	}
	buffer->frames[now_idx].is_pinned--;
	return 1;
#endif
}


int db_update(int table_id, int64_t key, char* value, int trx_id){
#ifndef SERIALIZER
	pthread_mutex_lock(&trx_mutex);
	if(aborted_trx.count(trx_id)){
		pthread_mutex_unlock(&trx_mutex);
		return ABORT;
	}
	pthread_mutex_unlock(&trx_mutex);
	int idx;
	undo_log_t* undo = NULL;
	while(true){
		pthread_mutex_lock(&buffer_mutex);
		pagenum_t page_no = find_leaf(table_id, key);
		if(!page_no){
			pthread_mutex_unlock(&buffer_mutex);
			return FAIL;
		}

		idx = buf_get_page(table_id, page_no);
		pthread_mutex_lock(&(buffer->frames[idx].page_mutex));
		page_t* page = buffer->frames[idx].page;
		for(int i = 0; i < page->header.num_keys; i++){
			if(key==page->records[i].key){
				undo = new undo_log_t(key, page->records[i].value);
				break;	
			}
		}
	
		pthread_mutex_unlock(&buffer_mutex);

		lock_state state = acquire_lock(table_id, page_no, trx_id, EXCLUSIVE, undo);

		if(state == LOCK_SUCCESS){
			break;
		}else if(state == LOCK_WAITING){	
			pthread_mutex_unlock(&(buffer->frames[idx].page_mutex));
			pthread_mutex_lock(&trx_mutex);
			auto trx = trx_table[trx_id];
			trx->state = WAITING;
			pthread_cond_wait(&(trx->trx_cond), &trx_mutex);
			pthread_mutex_unlock(&trx_mutex);
		}else{
			pthread_mutex_unlock(&(buffer->frames[idx].page_mutex));
			abort_trx(trx_id);
			return ABORT;
		}
	}
	page_t* page = buffer->frames[idx].page;
	buffer->frames[idx].is_pinned++;
	for(int i = 0; i < page->header.num_keys; i++){
		if(key==page->records[i].key){
			memcpy(page->records[i].value, value, 120);
			buffer->frames[idx].is_dirty = 1;
			buffer->frames[idx].is_pinned--;
			pthread_mutex_unlock(&(buffer->frames[idx].page_mutex));
			return SUCCESS;
		}
	}
	
	buffer->frames[idx].is_pinned--;
	pthread_mutex_unlock(&(buffer->frames[idx].page_mutex));
	
	return FAIL;
#endif

#ifdef SERIALIZER
	if (!buffer_allocated) {
		return 1;
	}
	if (table_id <= 0 || table_id > 10) {
		return 1;
	}
	if (!id_table || id_table->id[table_id] == -1) {
		return 1;
	}
	int i = 0;
	pagenum_t pagenum = find_leaf(table_id, key);
	page_t* n;
	if (!pagenum) {
		return 1;
	}
	int now_idx = buf_get_page(table_id, pagenum);
	buffer->frames[now_idx].is_pinned++;
	n = buffer->frames[now_idx].page;
	for (i = 0; i < n->header.num_keys; i++) {
		if (n->records[i].key == key) {
			n->header.page_LSN = make_new_log(trx_id, UPDATE, table_id, pagenum, i, n->records[i].value, value);
			memcpy(n->records[i].value, value, 120);
			buffer->frames[now_idx].is_pinned--;
			return 0;
		}
	}
	buffer->frames[now_idx].is_dirty = 1;
	buffer->frames[now_idx].is_pinned--;
	return 1;
#endif
}
