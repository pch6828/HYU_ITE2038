#include"log_manager.h"
#include"trx_api.h"
#include"buffer_manager.h"
#include<stdlib.h>
#include<unistd.h>
#include<unordered_set>
#include<string>
#include<fcntl.h>
#include<string.h>
#include<iostream>

using namespace std;

volatile int64_t last_LSN = 0;
std::vector<log_t*>log_buffer;
volatile int flushed_idx;
volatile int log_fd;
unordered_set<int>loser_list;

int64_t make_new_log(int trx_id, int type) {
	log_t* log = (log_t*)malloc(sizeof(log_t));
	log->prev_LSN = last_LSN;
	last_LSN += 40;
	log->LSN = last_LSN;
	log->trx_id = trx_id;
	log->type = type;
	log_buffer.push_back(log);
	
	return log->LSN;
}

int64_t make_new_log(int trx_id, int type, int table_id, int page_no, int idx, char* old_img, char* new_img) {
	log_t* log = (log_t*)malloc(sizeof(log_t));
	log->prev_LSN = last_LSN;
	last_LSN += 280;
	log->LSN = last_LSN;
	log->trx_id = trx_id;
	log->type = type;
	log->data_len = 120;
	log->table_id = table_id;
	log->page_no = page_no;
	log->offset = get_record_offset(idx);
	memcpy(log->old_img, old_img, 120);
	memcpy(log->new_img, new_img, 120);

	log_buffer.push_back(log);
	return log->LSN;
}

void flush_log() {
	for (flushed_idx; flushed_idx < log_buffer.size(); flushed_idx++) {
		log_t* log = log_buffer[flushed_idx];
		if(log->type == UPDATE){
			write(log_fd, log, 280);
		}else{
			write(log_fd, log, 40);
		}
	}
};

int get_record_offset(int idx) {
	return (idx + 1) * 128 + 8;
}

int get_record_idx(int offset) {
	return (offset - 8) / 128 - 1;
}


void rollback(int trx_id) {
	for (int it = log_buffer.size() - 1; it >= 0; it--) {
		log_t* log = log_buffer[it];
		if (log->trx_id == trx_id) {
			if (log->type == UPDATE) {
				int i = get_record_idx(log->offset);
				make_new_log(trx_id, UPDATE, log->table_id, log->page_no, i, log->new_img, log->old_img);
				int now_idx = buf_get_page(log->table_id, log->page_no);

				buffer->frames[now_idx].is_pinned++;
				page_t* n = buffer->frames[now_idx].page;
				buffer->frames[now_idx].is_dirty = 1;
				memcpy(n->records[i].value, log->old_img, 120);
				buffer->frames[now_idx].is_pinned--;
			}
			else if (log->type == BEGIN) {
				break;
			}
		}
	}
}

int recover() {
	log_fd = open("log_file.db", O_RDWR | O_SYNC | O_CREAT | O_APPEND, 0777);
	if (log_fd < 0) {
		return 1;
	}
	log_t* log = (log_t*)malloc(sizeof(log_t));
	memset(log, 0, sizeof(log_t));
	while (pread(log_fd, log, 280, last_LSN) > 0) {
		flushed_idx++;
		if (log->type == BEGIN) {
			loser_list.insert(log->trx_id);
			make_new_log(log->trx_id, log->type);
		}
		else if (log->type == UPDATE) {
			string filename = "DATA" + to_string(log->table_id);
			open_table(const_cast<char*>(filename.c_str()));
			int i = get_record_idx(log->offset);
			int now_idx = buf_get_page(log->table_id, log->page_no);
			int64_t LSN =  make_new_log(log->trx_id, log->type, log->table_id, log->page_no, i, log->old_img, log->new_img);
			buffer->frames[now_idx].is_pinned++;
			page_t* n = buffer->frames[now_idx].page;
			if (n->header.page_LSN < log->LSN) {
				n->header.page_LSN = LSN;
				memcpy(n->records[i].value, log->new_img, 120);
				buffer->frames[now_idx].is_dirty = 1;
			}
			buffer->frames[now_idx].is_pinned--;
		}
		else if (log->type == COMMIT) {
			loser_list.erase(log->trx_id);
			make_new_log(log->trx_id, log->type);
		}else if(log->type == ABORT){
			make_new_log(log->trx_id, log->type);
		}
		memset(log, 0, sizeof(log_t));
	}
	for (int it = log_buffer.size() - 1; it >= 0; it--) {
		log_t* log = log_buffer[it];
		if (loser_list.count(log->trx_id)) {
			if (log->type == UPDATE) {
				int i = get_record_idx(log->offset);
				make_new_log(log->trx_id, UPDATE, log->table_id, log->page_no, i, log->new_img, log->old_img);
				int now_idx = buf_get_page(log->table_id, log->page_no);

				buffer->frames[now_idx].is_pinned++;
				page_t* n = buffer->frames[now_idx].page;
				if (n->header.page_LSN >= log->LSN) {
					n->header.page_LSN = log->LSN - 1;
					memcpy(n->records[i].value, log->old_img, 120);
					buffer->frames[now_idx].is_dirty = 1;
				}
				buffer->frames[now_idx].is_pinned--;
			}
			else if (log->type == BEGIN) {
				loser_list.erase(log->trx_id);
			}
		}
	}
	free(log);
	return 0;
};

void print_log(){
	for (auto log : log_buffer) {
		cout << log->prev_LSN << " " << log->LSN <<" "<< log->trx_id << " " << log->type;
		if (log->type == UPDATE) {
			cout << " " << log->old_img << " " << log->new_img;
		}
		cout << endl;
	}
}
