#pragma gcc optimize("o2")
#include "join.h"
#include "buffer_manager.h"
#include "bpt.h"
#include <stdio.h>
#include <inttypes.h>

int join_table(int tid1, int tid2, char* pathname){
	if(tid1>10 || tid1<=0 || tid2>10 || tid2<=0){
		return 1;
	}
	if(!id_table || id_table->id[tid1]==-1||id_table->id[tid2]==-1){
		return 1;
	}
	FILE* fp = fopen(pathname, "w");
	if(!fp){
		return 1;
	}
	if(!buffer_allocated){
                return 1;
        }
        int page_idx1, page_idx2;
        buf_read_header(tid1);
	buf_read_header(tid2);
        page_t* c1, * c2;
        if (buffer->header[tid1]->root_page_no) {
                page_idx1 = buf_get_page(tid1, buffer->header[tid1]->root_page_no);
		buffer->frames[page_idx1].is_pinned++;
                c1 = buffer->frames[page_idx1].page;
                while (!c1->header.is_leaf) {
			buffer->frames[page_idx1].is_pinned--;
                        page_idx1 = buf_get_page(tid1, c1->leftmost_child_no);
			buffer->frames[page_idx1].is_pinned++;
                        c1 = buffer->frames[page_idx1].page;
                }
        }else{
		fclose(fp);
		return 0;
	}

        if (buffer->header[tid2]->root_page_no) {
                page_idx2 = buf_get_page(tid2, buffer->header[tid2]->root_page_no);
		buffer->frames[page_idx2].is_pinned++;
                c2 = buffer->frames[page_idx2].page;
        
                while (!c2->header.is_leaf) {
			buffer->frames[page_idx2].is_pinned--;
                        page_idx2 = buf_get_page(tid2, c2->leftmost_child_no);
			buffer->frames[page_idx2].is_pinned++;
                        c2 = buffer->frames[page_idx2].page;
                }
        }else{
                fclose(fp);
                return 0;
        }
	
	int r1 = 0, r2 = 0;
	while(true){
		if(r1 == c1->header.num_keys){
			if(c1->right_sibling_no){
                        	buffer->frames[page_idx1].is_pinned--;
                        	page_idx1 = buf_get_page(tid1, c1->right_sibling_no);
                        	buffer->frames[page_idx1].is_pinned++;
                        	c1 = buffer->frames[page_idx1].page;
				r1 = 0;
			}else{
                        	buffer->frames[page_idx1].is_pinned--;
				break;
			}
		}
                if(r2 == c2->header.num_keys){
                        if(c2->right_sibling_no){
                                buffer->frames[page_idx2].is_pinned--;
                                page_idx2 = buf_get_page(tid2, c2->right_sibling_no);
                                buffer->frames[page_idx2].is_pinned++;
                                c2= buffer->frames[page_idx2].page;
				r2 = 0;
                        }else{
                                buffer->frames[page_idx2].is_pinned--;
                                break;
                        }
                }

		if(c1->records[r1].key == c2->records[r2].key){
			fprintf(fp, "%" PRId64 ",%s,%" PRId64 ",%s\n", c1->records[r1].key, c1->records[r1].value, c2->records[r2].key, c2->records[r2].value);
			r1++;
			r2++;
		}else if(c1->records[r1].key > c2->records[r2].key){
			r2++;
		}else if(c1->records[r1].key < c2->records[r2].key){
			r1++;
		}
	}

	fclose(fp);
	
	return 0;
}
