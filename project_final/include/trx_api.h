#ifndef __TRX_API_H__
#define __TRX_API_H__

#include<inttypes.h>
#include<stdint.h>

int db_find(int table_id, int64_t key, char* value, int trx_id);
int db_update(int table_id, int64_t key, char* value, int trx_id);

#endif /*__TRX_API_H__*/
