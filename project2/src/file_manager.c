#include<fcntl.h>
#include<unistd.h>
#include<stdlib.h>
#include<errno.h>
#include<stdio.h>
#include<string.h>
#include"file_manager.h"

//open table
int open_table(char* pathname){
	int flag;

	db = open(pathname, O_RDWR|O_SYNC|O_CREAT,0777);
	if(db < 0){
		return -1;
	}

	header = (header_page_t*)malloc(sizeof(header_page_t));
	memset(header, 0, sizeof(header_page_t));
	flag = file_read_header();
	if(header->number_of_pages==0||flag!=4096){
		header->free_page_no = 0;
		header->root_page_no = 0;
		header->number_of_pages = 1;
		file_write_header();
	}
	return db;
}

//new page allocation
pagenum_t file_alloc_page(){
	int flag;
	page_t* page;
	file_read_header();
	pagenum_t pagenum = header->free_page_no;
	page = (page_t*)malloc(sizeof(page_t));
	if(pagenum == 0){
		pagenum = header->number_of_pages++;
	}else{
		file_read_page(pagenum, page);
		header->free_page_no = page->header.parent_page_no;
	}
	file_write_header();
	free(page);
	return pagenum;
}

//empty page deallocation
void file_free_page(pagenum_t pagenum){
	int flag;
	page_t* page;
	file_read_header();
	
	page = (page_t*)malloc(sizeof(page_t));
	file_read_page(pagenum, page);
	page->header.parent_page_no = header->free_page_no;
	header->free_page_no = pagenum;

	file_write_page(pagenum, page);
	file_write_header();
	free(page);
}

//wrapper funcion for pread
void file_read_page(pagenum_t pagenum, page_t* dest){
	int flag;
    flag = pread(db, dest, PAGE_SIZE, pagenum*PAGE_SIZE);
    if(flag<0){
           //perror("while reading page at given pagenum");
    }
	
}

//wrapper functionfor pwrite
void file_write_page(pagenum_t pagenum, const page_t* src){
	int flag;
	flag = pwrite(db, src, PAGE_SIZE, pagenum*PAGE_SIZE);
	if(flag<PAGE_SIZE){
		perror("while writing page into given pagenum");
	}
}

//read header page
int file_read_header() {
	int flag;
	flag = pread(db, header, PAGE_SIZE, 0);
	return flag;
};

//write header page
void file_write_header() {
	int flag;
	flag = pwrite(db, header, PAGE_SIZE, 0);
	if (flag < 0) {
		perror("while writing header");
	}
};
