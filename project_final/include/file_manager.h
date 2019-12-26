#ifndef __FILE_MANAGER_H__
#define __FILE_MANAGER_H__

#include<stdint.h>

#define PAGE_SIZE 4096

#define HEADERPAGE_NO 0

//type definition for implementing Disk-based B+Tree
typedef uint64_t pagenum_t;

typedef struct{
	int64_t key;
	char value[120];
}record_t;

typedef struct{
	pagenum_t free_page_no;
	pagenum_t root_page_no;
	uint64_t number_of_pages;
	char reserved[4072];
}header_page_t;

typedef struct{
	pagenum_t parent_page_no;
	int is_leaf;
	int num_keys;
	char reserved0[8];
	int64_t page_LSN;
	char reserved1[84];
}page_header_t;

typedef struct{
	int64_t key;
	pagenum_t child_no;
}branch_factor_t;

typedef struct{
	page_header_t header;
	union{
		pagenum_t leftmost_child_no;
		pagenum_t right_sibling_no;
	};
	union{
		record_t records[31];
		branch_factor_t edge[248];
	};
}page_t;

//global variable for header page and root page
//global variable for opened file;

//function declaration for FILE MANAGER API
int open_db(char* pathname);
void close_db(int db);
pagenum_t file_alloc_page(int db);
void file_free_page(int db, pagenum_t pagenum);
int file_read_page(int db, pagenum_t pagenum, page_t* dest);
void file_write_page(int db, pagenum_t pagenum, const page_t* src);

#endif /*__FILE_MANAGER_H__*/
