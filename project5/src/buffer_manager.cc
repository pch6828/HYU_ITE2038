#pragma gcc optimize("o2")
#include "buffer_manager.h"
#include "file_manager.h"
#include <string.h>
#include <stdlib.h>
#include <iostream>

using namespace std;

id_table_t* id_table = NULL;
buffer_table_t* buffer = NULL;
volatile int buffer_allocated = 0;
pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER;

int init_db(int buf_num) {
	buffer_allocated = 1;
	buffer = (buffer_table_t*)malloc(sizeof(buffer_table_t));
	if (!buffer) {
		return 1;
	}else if(buf_num<=0){
		return 1;
	}
	buf_num = (buf_num > 10 ? buf_num:10);
	buffer->frames = (frame_t*)malloc(sizeof(frame_t) * buf_num);
	if (!buffer->frames) {
		return 1;
	}
	for (int i = 0; i < buf_num; i++) {
		buffer->frames[i].page = (page_t*)malloc(sizeof(page_t));
		buffer->frames[i].in_use = 0;
		buffer->frames[i].is_pinned = 0;
		buffer->frames[i].ref = 0;
		buffer->frames[i].is_dirty = 0;
		buffer->frames[i].page_mutex = PTHREAD_MUTEX_INITIALIZER;
	}
	for (int i = 1; i <= 10; i++){
		buffer->header[i] = (header_page_t*)malloc(sizeof(header_page_t));
		memset(buffer->header[i], 0, 4096);
		buffer->header_read[i] = 0;
	}
	buffer->hashtable = new unordered_map<pair<int, pagenum_t>, int, Hash>();
	buffer->size = buf_num;
	buffer->now_idx = 0;
	return 0;
}

int open_table(char* pathname) {
	int res;
	if (!id_table) {
		id_table = (id_table_t*)malloc(sizeof(id_table_t));
		if (!id_table) {
			return -1;
		}
		memset(id_table->id, -1, sizeof(id_table->id));
		id_table->cnt = 0;

		for(int i = 1; i <=10; i++){
			id_table->pathname[i] = NULL;
		}
	}

	if (id_table->cnt == 10) {
		return -1;
	}
	else {
		for (int i = 1; i <= 10; i++){
			if(id_table->pathname[i]&&!strcmp(pathname, id_table->pathname[i])){
				return -1;
			}
		}
		for (int i = 1; i <= 10; i++) {
			if (id_table->id[i] == -1) {
				res = id_table->id[i] = open_db(pathname);
				if (res == -1) {
					return res;
				}
				else {
					id_table->cnt++;
					id_table->pathname[i] = (char*)malloc(strlen(pathname)+1);
					strcpy(id_table->pathname[i], pathname);
					return i;
				}
			}
		}
	}
}

void close_table(int table_id){
	if(buffer_allocated){
	        for(int i = 0; i < buffer->size; i++){
	                if(buffer->frames[i].table_id == table_id&&buffer->frames[i].in_use){
				if(buffer->frames[i].is_dirty){
					file_write_page(id_table->id[table_id], buffer->frames[i].page_no, buffer->frames[i].page);
				}
				buffer->hashtable->erase({buffer->frames[i].table_id, buffer->frames[i].page_no});
	                        buffer->frames[i].table_id = 0;
	                        buffer->frames[i].page_no = 0;
				buffer->frames[i].in_use = 0;
				buffer->frames[i].ref = 0;
				buffer->frames[i].is_pinned = 0;
				buffer->frames[i].is_dirty = 0;
                	}
        	}
	}
	if (id_table->cnt&&id_table->id[table_id]!=-1) {
		close_db(id_table->id[table_id]);
		id_table->id[table_id] = -1;
		id_table->cnt--;
		buffer->header_read[table_id] = 0;
		memset(buffer->header[table_id], 0, 4096);
		free(id_table->pathname[table_id]);
	}
}

int shutdown_db() {
	if (buffer_allocated) {
		for (int i = 1; i <= 10; i++) {
			if (id_table->id[i] != -1) {
				file_write_page(id_table->id[i], 0, (page_t*)buffer->header[i]);
			}
			free(buffer->header[i]);
		}
		for (int i = 0; i < buffer->size; i++) {
			if (buffer->frames[i].in_use && buffer->frames[i].is_dirty) {
				file_write_page(id_table->id[buffer->frames[i].table_id], buffer->frames[i].page_no, buffer->frames[i].page);
			}
			free(buffer->frames[i].page);
		}
		free(buffer->frames);
		delete buffer->hashtable;
		free(buffer);
		buffer = NULL;
		buffer_allocated = 0;
		return 0;
	}
	else {
		return 1;
	}
}

int buf_alloc_page(int table_id) {
	int cnt = 0;
	buf_read_header(table_id);

	while (1) {
		cnt++;
		buffer->now_idx++;
		buffer->now_idx %= buffer->size;
		if (buffer->frames[buffer->now_idx].in_use == 0) {
			memset(buffer->frames[buffer->now_idx].page, 0, 4096);
			buffer->frames[buffer->now_idx].in_use = 1;
			buffer->frames[buffer->now_idx].is_dirty = buffer->frames[buffer->now_idx].is_pinned = 0;
			buffer->frames[buffer->now_idx].table_id = table_id;
			buffer->frames[buffer->now_idx].page_no = file_alloc_page(id_table->id[table_id]);
			buffer->hashtable->insert({{table_id, buffer->frames[buffer->now_idx].page_no}, buffer->now_idx});
			buffer->frames[buffer->now_idx].ref = 1;
			buf_read_header(buffer->frames[buffer->now_idx].table_id);
			return buffer->now_idx;
		}
		else if (buffer->frames[buffer->now_idx].is_pinned == 0) {
			if (buffer->frames[buffer->now_idx].ref == 0) {
				if (buffer->frames[buffer->now_idx].is_dirty) {
					file_write_page(id_table->id[buffer->frames[buffer->now_idx].table_id], buffer->frames[buffer->now_idx].page_no, buffer->frames[buffer->now_idx].page);
				}
				buffer->hashtable->erase({buffer->frames[buffer->now_idx].table_id, buffer->frames[buffer->now_idx].page_no});
				memset(buffer->frames[buffer->now_idx].page, 0, 4096);
				buffer->frames[buffer->now_idx].in_use = 1;
				buffer->frames[buffer->now_idx].is_dirty = buffer->frames[buffer->now_idx].is_pinned = 0;
				buffer->frames[buffer->now_idx].table_id = table_id;
				buffer->frames[buffer->now_idx].page_no = file_alloc_page(id_table->id[table_id]);
				buffer->hashtable->insert({{table_id, buffer->frames[buffer->now_idx].page_no}, buffer->now_idx});
				buffer->frames[buffer->now_idx].ref = 1;
				buf_read_header(buffer->frames[buffer->now_idx].table_id);
				return buffer->now_idx;
			}
			else {
				buffer->frames[buffer->now_idx].ref = 0;
			}
		}
	}
}

void buf_free_page(int buffer_idx) {
	buf_read_header(buffer->frames[buffer_idx].table_id);
	int table_id = buffer->frames[buffer_idx].table_id;
	buffer->hashtable->erase({table_id, buffer->frames[buffer_idx].page_no});
	file_free_page(id_table->id[buffer->frames[buffer_idx].table_id], buffer->frames[buffer_idx].page_no);
	buf_read_header(buffer->frames[buffer_idx].table_id);
	
	memset(buffer->frames[buffer_idx].page, 0, 4096);
	buffer->frames[buffer_idx].in_use = 0;
	buffer->frames[buffer_idx].is_dirty = buffer->frames[buffer_idx].is_pinned = 0;
	buffer->frames[buffer_idx].table_id = 0;
	buffer->frames[buffer_idx].page_no = 0;
	buffer->frames[buffer_idx].ref = 0;
}

int buf_get_page(int table_id, pagenum_t page_no) {
	if(buffer->hashtable->count({table_id, page_no})){
		int ret = buffer->hashtable->at({table_id, page_no});
		buffer->frames[ret].ref = 1;
		return ret;
	}

	int cnt = 0;
	while (1) {
		cnt++;
		buffer->now_idx++;
		buffer->now_idx %= buffer->size;

		if (buffer->frames[buffer->now_idx].in_use == 0) {
			file_read_page(id_table->id[table_id], page_no, buffer->frames[buffer->now_idx].page);
			buffer->frames[buffer->now_idx].in_use = 1;
			buffer->frames[buffer->now_idx].is_dirty = buffer->frames[buffer->now_idx].is_pinned = 0;
			buffer->frames[buffer->now_idx].table_id = table_id;
			buffer->frames[buffer->now_idx].page_no = page_no;
			buffer->frames[buffer->now_idx].ref = 1;
			buffer->hashtable->insert({{table_id, buffer->frames[buffer->now_idx].page_no}, buffer->now_idx});
			return buffer->now_idx;
		}
		else if (buffer->frames[buffer->now_idx].is_pinned == 0) {
			if (buffer->frames[buffer->now_idx].ref == 0) {
				if (buffer->frames[buffer->now_idx].is_dirty) {
					file_write_page(id_table->id[buffer->frames[buffer->now_idx].table_id], buffer->frames[buffer->now_idx].page_no, buffer->frames[buffer->now_idx].page);
				}
				buffer->hashtable->erase({buffer->frames[buffer->now_idx].table_id, buffer->frames[buffer->now_idx].page_no});
				file_read_page(id_table->id[table_id], page_no, buffer->frames[buffer->now_idx].page);
				buffer->frames[buffer->now_idx].in_use = 1;
				buffer->frames[buffer->now_idx].is_dirty = buffer->frames[buffer->now_idx].is_pinned = 0;
				buffer->frames[buffer->now_idx].table_id = table_id;
				buffer->frames[buffer->now_idx].page_no = page_no;
				buffer->frames[buffer->now_idx].ref = 1;
				buffer->hashtable->insert({{table_id, buffer->frames[buffer->now_idx].page_no}, buffer->now_idx});

				return buffer->now_idx;
			}
			else {
				buffer->frames[buffer->now_idx].ref = 0;
			}
		}
	}
}

void buf_read_header(int table_id) {
	file_read_page(id_table->id[table_id], 0, (page_t*)buffer->header[table_id]);
	buffer->header_read[table_id] = 1;
}

void buf_write_header(int table_id) {
	file_write_page(id_table->id[table_id], 0, (page_t*)buffer->header[table_id]);
}
