#ifndef __BUFFER_MANAGER_H__
#define __BUFFER_MANAGER_H__

#include "file_manager.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

typedef struct {
	page_t* page;
	int table_id;
	pagenum_t page_no;
	int is_dirty;
	int is_pinned;
	int in_use;
	int ref;
}frame_t;

typedef struct {
	header_page_t* header[11];
	frame_t* frames;
	int size;
	int now_idx;
}buffer_table_t;


typedef struct {
	int id[11];
	char* pathname[11];
	int cnt;
}id_table_t;

id_table_t* id_table;
buffer_table_t* buffer;
int buffer_allocated;

int buf_alloc_page(int table_id);
void buf_free_page(int buffer_idx);
int buf_get_page(int table_id, pagenum_t page_no);
void buf_write_header(int table_id);
void buf_read_header(int table_id);

int init_db(int buf_num);
void close_table(int table_id);
int open_table(char* pathname);
int shutdown_db();

#endif
