#pragma gcc optimize("o2")
#include<fcntl.h>
#include<unistd.h>
#include<stdlib.h>
#include<errno.h>
#include<stdio.h>
#include<string.h>
#include"file_manager.h"


//open table
int open_db(char* pathname){
	int flag;

	int db = open(pathname, O_RDWR|O_SYNC|O_CREAT,0777);
	if(db < 0){
		return -1;
	}

	header_page_t* header = (header_page_t*)malloc(sizeof(header_page_t));
	memset(header, 0, sizeof(header_page_t));
	flag = file_read_page(db, 0, (page_t*)header);
	if(header->number_of_pages==0||flag!=4096){
		header->free_page_no = 0;
		header->root_page_no = 0;
		header->number_of_pages = 1;
		file_write_page(db, 0, (page_t*)header);
	}
	free(header);
	return db;
}

void close_db(int db) {
	close(db);
};

//new page allocation
pagenum_t file_alloc_page(int db){
	int flag;
	page_t* page;
	header_page_t* header = (header_page_t*)malloc(sizeof(header_page_t));
	file_read_page(db, 0, (page_t*)header);
	pagenum_t pagenum = header->free_page_no;
	page = (page_t*)malloc(sizeof(page_t));
	if(pagenum == 0){
		pagenum = header->number_of_pages++;
	}else{
		file_read_page(db, pagenum, page);
		header->free_page_no = page->header.parent_page_no;
	}
	file_write_page(db, 0, (page_t*)header);
	free(page);
	free(header);
	return pagenum;
}

//empty page deallocation
void file_free_page(int db, pagenum_t pagenum){
	int flag;
	page_t* page;
	header_page_t* header = (header_page_t*)malloc(sizeof(header_page_t));
	file_read_page(db, 0, (page_t*)header);
	
	page = (page_t*)malloc(sizeof(page_t));
	file_read_page(db, pagenum, page);
	page->header.parent_page_no = header->free_page_no;
	header->free_page_no = pagenum;

	file_write_page(db, pagenum, page);
	file_write_page(db, 0, (page_t*)header);
	free(page);
	free(header);
}

//wrapper funcion for pread
int file_read_page(int db, pagenum_t pagenum, page_t* dest){
	int flag;
    flag = pread(db, dest, PAGE_SIZE, pagenum*PAGE_SIZE);
	
	return flag;
}

//wrapper functionfor pwrite
void file_write_page(int db, pagenum_t pagenum, const page_t* src){
	int flag;
	flag = pwrite(db, src, PAGE_SIZE, pagenum*PAGE_SIZE);
	if(flag<PAGE_SIZE){
		perror("while writing page into given pagenum");
	}
}

