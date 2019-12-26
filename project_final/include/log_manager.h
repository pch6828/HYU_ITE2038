#ifndef __LOG_MANAGER__
#define __LOG_MANAGER__

#include<vector>
#include<stdint.h>
#include<inttypes.h>

typedef struct {
	int64_t LSN;
	int64_t prev_LSN;
	int trx_id;
	int type;
	int table_id;
	int page_no;
	int offset;
	int data_len;
	char old_img[120];
	char new_img[120];
}log_t;

#define BEGIN 0
#define UPDATE 1
#define COMMIT 2
#define ABORT 3

extern volatile int64_t last_LSN;
extern std::vector<log_t*>log_buffer;
extern volatile int flushed_idx;
extern volatile int log_fd;

int64_t make_new_log(int trx_id, int type);
int64_t make_new_log(int trx_id, int type, int table_id, int page_no, int idx, char* old_img, char* new_img);
void flush_log();
int get_record_offset(int idx);
int get_record_idx(int offset);
void rollback(int trx_id);
int recover();
void print_log();

#endif
