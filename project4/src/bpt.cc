#define Version "1.14"
#pragma gcc optimize("o2")
#include "bpt.h"
#include "file_manager.h"
#include "buffer_manager.h"
#include <string.h>
#include <inttypes.h>
 
int order = LEAF_ORDER;
int internal_order = INTERNAL_ORDER;

bool verbose_output = false;

Queue make_new_queue() {
	Queue q = (Queue)malloc(sizeof(struct Node));
	q->next = NULL;
	return q;
}
int is_empty(Queue queue) {
	return queue->next == NULL;
}
void enqueue(pagenum_t new_pagenum, Queue queue) {
	Queue c = queue;
	Queue end_c = (Queue)malloc(sizeof(struct Node));
	while (c->next != NULL) {
		c = c->next;
	}
	end_c->id = new_pagenum;
	end_c->next = NULL;
	c->next = end_c;
}

pagenum_t dequeue(Queue queue) {
	Queue temp = queue->next;
	pagenum_t ret = temp->id;
	queue->next = temp->next;
	free(temp);
	temp = NULL;
	return ret;
}

void print_leaves(int tid) {
	if(!buffer_allocated){
		return;
	}
	int i, now_idx;
	if(buffer->header_read[tid] == 0){
		buf_read_header(tid);
	}
	page_t* c;
	if (buffer->header[tid]->root_page_no) {
		now_idx = buf_get_page(tid, buffer->header[tid]->root_page_no);
		c = buffer->frames[now_idx].page;
		while (!c->header.is_leaf) {
			now_idx = buf_get_page(tid, c->leftmost_child_no);
			c = buffer->frames[now_idx].page;
		}
	}
	else {
		printf("Empty tree.\n");
		return;
	}
	while (true) {
		for (i = 0; i < c->header.num_keys; i++) {
			printf("%" PRId64 " ", c->records[i].key);
		}
		if (c->right_sibling_no) {
			printf(" | ");
			now_idx = buf_get_page(tid, c->right_sibling_no);
			c = buffer->frames[now_idx].page;
		}
		else
			break;
	}
	printf("\n");
}

int height(int tid) {
	int h = 0;
	page_t* c;
	if(buffer->header_read[tid] == 0){
		buf_read_header(tid);
	}
	if (buffer->header[tid]->root_page_no) {
		int now_idx = buf_get_page(tid, buffer->header[tid]->root_page_no);
		c = buffer->frames[now_idx].page;
		while (!c->header.is_leaf) {
			now_idx = buf_get_page(tid, c->leftmost_child_no);
			c = buffer->frames[now_idx].page;
			h++;
		}
	}
	return h;
}

int path_to_root(int tid, pagenum_t pagenum) {
	int length = 0;
	if(buffer->header_read[tid] == 0){
		buf_read_header(tid);
	}
	pagenum_t root = buffer->header[tid]->root_page_no;
	page_t* c;
	int now_idx = buf_get_page(tid, pagenum);
	c = buffer->frames[now_idx].page;
	while (pagenum != root) {
		pagenum = c->header.parent_page_no;
		now_idx = buf_get_page(tid, pagenum);
		c = buffer->frames[now_idx].page;
		length++;
	}
	return length;
}

void print_tree(int tid) {
	if(!buffer_allocated){
		return;
	}
	pagenum_t now;
	int now_idx;
	if(buffer->header_read[tid] == 0){
		buf_read_header(tid);
	}
	page_t* n;
	if (!buffer->header[tid]->root_page_no) {
		printf("Empty tree.\n");
		return;
	}
	Queue queue = make_new_queue();
	enqueue(buffer->header[tid]->root_page_no, queue);
	int h = path_to_root(tid, buffer->header[tid]->root_page_no);
	while (!is_empty(queue)) {
		now = dequeue(queue);
		int temp_h = path_to_root(tid, now);
		if (h != temp_h) {
			h = temp_h;
			printf("\n");
		}
		now_idx = buf_get_page(tid, now);
		n = buffer->frames[now_idx].page;
		if (!n->header.is_leaf) {
			enqueue(n->leftmost_child_no, queue);
		}
		printf("<%" PRId64 "/%" PRIu64 "> ", n->header.parent_page_no, now); 
		for (int i = 0; i < n->header.num_keys; i++) {
			if (!n->header.is_leaf) {
				printf("%" PRId64 " ", n->edge[i].key);
				enqueue(n->edge[i].child_no, queue);
			}
			else {
				printf("%" PRId64 " ", n->records[i].key);
			}
		}
		printf("| ");
		fflush(stdout);
	}
	printf("\n");
}

pagenum_t find_leaf(int tid, int64_t key) {
	int i;
	pagenum_t now;
	page_t* c;
	if(buffer->header_read[tid] == 0){
		buf_read_header(tid);
	}
	if (!buffer->header[tid]->root_page_no) {
		return 0;
	}
	int now_idx = buf_get_page(tid, buffer->header[tid]->root_page_no);
	buffer->frames[now_idx].is_pinned++;
	now = buffer->header[tid]->root_page_no;
	c = buffer->frames[now_idx].page;
	while (!c->header.is_leaf) {
		bool chk = false;
		for (int i = c->header.num_keys - 1; i >= 0; i--) {
			if (c->edge[i].key <= key) {
				buffer->frames[now_idx].is_pinned--;
				now = c->edge[i].child_no;
				chk = true;
				break;
			}
		}
		if (!chk) {
			buffer->frames[now_idx].is_pinned--;
			now = c->leftmost_child_no;
		}
		now_idx = buf_get_page(tid, now);
                buffer->frames[now_idx].is_pinned++;
                c = buffer->frames[now_idx].page;
	}
	buffer->frames[now_idx].is_pinned--;
	return now;
}

int db_find(int tid, int64_t key, char* dest) {
	if(!buffer_allocated){
		return 1;
	}
	if(tid<=0||tid>10){
		return 1;
	}
	if(!id_table||id_table->id[tid]==-1){
		return 1;
	}
	int i = 0;
	pagenum_t pagenum = find_leaf(tid, key);
	page_t* n;
	if (!pagenum) {
		return 1;
	}
	int now_idx = buf_get_page(tid, pagenum);
	buffer->frames[now_idx].is_pinned++;
	n = buffer->frames[now_idx].page;
	for (i = 0; i < n->header.num_keys; i++) {
		if (n->records[i].key == key) {
			memcpy(dest, n->records[i].value, 120);
			buffer->frames[now_idx].is_pinned--;
			return 0;
		}
	}
	buffer->frames[now_idx].is_pinned--;
	return 1;
}

// INSERTION
int cut(int length) {
	if (length % 2 == 0)
		return length / 2;
	else
		return length / 2 + 1;
}

record_t* make_record(int64_t key, char* value) {
	record_t* new_record = (record_t*)malloc(128);
	new_record->key = key;
	memcpy(new_record->value, value, 120);
	return new_record;
}

page_t* make_node(void) {
	page_t* new_page = (page_t*)malloc(sizeof(page_t));
	memset(new_page, 0, 4096);
	new_page->header.is_leaf = 0;
	return new_page;
}

page_t* make_leaf(void) {
	page_t* leaf = make_node();

	leaf->header.is_leaf = 1;
	return leaf;
}

int get_left_index(page_t* parent, pagenum_t left_no) {
	int left_index = 0;
	if (left_no == parent->leftmost_child_no) {
		return left_index;
	}
	for (int i = 0; i < parent->header.num_keys; i++) {
		if (parent->edge[i].child_no == left_no) {
			return i + 1;
		}
	}
}

int insert_into_leaf(int tid, pagenum_t leaf_no, page_t* leaf, record_t* record) {
	int i, insertion_point;
	insertion_point = 0;
	while (insertion_point < leaf->header.num_keys && leaf->records[insertion_point].key < record->key)
		insertion_point++;
	for (i = leaf->header.num_keys; i > insertion_point; i--) {
		leaf->records[i] = leaf->records[i - 1];
	}
	leaf->records[insertion_point].key = record->key;
	memcpy(leaf->records[insertion_point].value, record->value, 120);
	leaf->header.num_keys++;
	return 0;
}

int insert_into_leaf_after_splitting(int tid, pagenum_t leaf_no, page_t* leaf, record_t* record) {
	page_t* new_leaf = make_leaf();
	record_t temp_records[33];
	memset(temp_records, 0, sizeof(temp_records));
	int insertion_index, split, i, j;
	int64_t new_key;
	insertion_index = 0;
	while (insertion_index < LEAF_ORDER - 1 && leaf->records[insertion_index].key < record->key)
		insertion_index++;
	for (i = 0, j = 0; i < leaf->header.num_keys; i++, j++) {
		if (j == insertion_index) j++;
		temp_records[j].key = leaf->records[i].key;
		memcpy(temp_records[j].value, leaf->records[i].value, 120);
	}
	temp_records[insertion_index].key = record->key;
	memcpy(temp_records[insertion_index].value, record->value, 120);
	free(record);
	leaf->header.num_keys = 0;
	split = cut(LEAF_ORDER - 1);
	for (i = 0; i < split; i++) {
		leaf->records[i].key = temp_records[i].key;
		memcpy(leaf->records[i].value, temp_records[i].value, 120);
		leaf->header.num_keys++;
	}
	for (i = split, j = 0; i < LEAF_ORDER; i++, j++) {
		new_leaf->records[j].key = temp_records[i].key;
		memcpy(new_leaf->records[j].value, temp_records[i].value, 120);
		new_leaf->header.num_keys++;
	}
	new_leaf->right_sibling_no = leaf->right_sibling_no;
	pagenum_t new_leaf_no;
	int new_leaf_idx = buf_alloc_page(tid);
	leaf->right_sibling_no = buffer->frames[new_leaf_idx].page_no;
	pagenum_t parent_no = leaf->header.parent_page_no;
	for (i = leaf->header.num_keys; i < 31; i++) {
		leaf->records[i].key = 0;
		memset(leaf->records[i].value, 0, 120);
	}
	for (i = new_leaf->header.num_keys; i < 31; i++) {
		new_leaf->records[i].key = 0;
		memset(new_leaf->records[i].value, 0, 120);
	}
	new_leaf->header.parent_page_no = leaf->header.parent_page_no;
	memcpy(buffer->frames[new_leaf_idx].page, new_leaf, 4096);
	buffer->frames[new_leaf_idx].is_pinned++;
	buffer->frames[new_leaf_idx].is_dirty = 1;
	free(new_leaf);
	new_leaf = buffer->frames[new_leaf_idx].page;
	new_leaf_no = buffer->frames[new_leaf_idx].page_no;
	new_key = new_leaf->records[0].key;
	
	int flag = insert_into_parent(tid, parent_no, leaf_no, leaf, new_key, new_leaf_no, new_leaf);
	buffer->frames[new_leaf_idx].is_pinned--;
	
	return flag;
}

int insert_into_node(int tid, pagenum_t parent_no, page_t* parent, int left_index, int64_t key, pagenum_t right_no, page_t* right) {
	int i;
	for (i = parent->header.num_keys; i > left_index; i--) {
		parent->edge[i] = parent->edge[i - 1];
	}
	parent->edge[left_index].key = key;
	parent->edge[left_index].child_no = right_no;
	parent->header.num_keys++;
	right->header.parent_page_no = parent_no;

	return 0;
}

int insert_into_node_after_splitting(int tid, pagenum_t old_node_no, page_t* old_node, int left_index, int64_t key, pagenum_t right_no, page_t* right) {
	int i, j, split;
	int64_t k_prime;
	page_t* new_node, * child;
	branch_factor_t temp_edge[250];
	memset(temp_edge, 0, sizeof(temp_edge));

	for (i = 0, j = 0; i < old_node->header.num_keys; i++, j++) {
		if (j == left_index) {
			j++;
		}
		temp_edge[j].key = old_node->edge[i].key;
		temp_edge[j].child_no = old_node->edge[i].child_no;
	}
	temp_edge[left_index].key = key;
	temp_edge[left_index].child_no = right_no;

	split = cut(INTERNAL_ORDER);
	new_node = make_node();
	old_node->header.num_keys = 0;
	for (i = 0; i < split; i++) {
		old_node->edge[i].key = temp_edge[i].key;
		old_node->edge[i].child_no = temp_edge[i].child_no;
		old_node->header.num_keys++;
	}
	for (i = split; i < INTERNAL_ORDER - 1; i++) {
		old_node->edge[i].key = 0;
		old_node->edge[i].child_no = 0;
	}
	k_prime = temp_edge[split].key;
	new_node->leftmost_child_no = temp_edge[split].child_no;
	new_node->header.num_keys = 0;
	for (i = split + 1, j = 0; temp_edge[i].child_no != 0; i++, j++) {
		new_node->edge[j].key = temp_edge[i].key;
		new_node->edge[j].child_no = temp_edge[i].child_no;
		new_node->header.num_keys++;
	}
	new_node->header.parent_page_no = old_node->header.parent_page_no;
	pagenum_t child_no = new_node->leftmost_child_no, new_node_no;

	int new_node_idx = buf_alloc_page(tid);
	buffer->frames[new_node_idx].is_pinned++;
	new_node_no = buffer->frames[new_node_idx].page_no;
	
	int child_idx = buf_get_page(tid, child_no);
	child = buffer->frames[child_idx].page;
	child->header.parent_page_no = new_node_no;
	buffer->frames[child_idx].is_dirty = 1;

	for (i = 0; i < new_node->header.num_keys; i++) {
		child_no = new_node->edge[i].child_no;
		child_idx = buf_get_page(tid, child_no);
		child = buffer->frames[child_idx].page;
		child->header.parent_page_no = new_node_no;
		buffer->frames[child_idx].is_dirty = 1;
	}
	memcpy(buffer->frames[new_node_idx].page, new_node, 4096);
	free(new_node);
	new_node = buffer->frames[new_node_idx].page;
	buffer->frames[new_node_idx].is_dirty = 1;

	int flag = insert_into_parent(tid, old_node->header.parent_page_no, old_node_no, old_node, k_prime, new_node_no, new_node);
	buffer->frames[new_node_idx].is_pinned--;
	return flag;
}

int insert_into_parent(int tid, pagenum_t parent_no, pagenum_t left_no, page_t* left, int64_t key, pagenum_t right_no, page_t* right) {
	int left_index, flag;
	page_t* parent;

	if (!parent_no) {
		return insert_into_new_root(tid, left_no, left, key, right_no, right);
	}

	int parent_idx = buf_get_page(tid, parent_no);
	parent = buffer->frames[parent_idx].page;
	buffer->frames[parent_idx].is_pinned++;
	left_index = get_left_index(parent, left_no);

	if (parent->header.num_keys < INTERNAL_ORDER - 1) {
		flag = insert_into_node(tid, parent_no, parent, left_index, key, right_no, right);
		buffer->frames[parent_idx].is_pinned--;
		buffer->frames[parent_idx].is_dirty = 1;
		return flag;
	}

	flag = insert_into_node_after_splitting(tid, parent_no, parent, left_index, key, right_no, right);
	buffer->frames[parent_idx].is_pinned--;
	buffer->frames[parent_idx].is_dirty = 1;
	return flag;
}

int insert_into_new_root(int tid, pagenum_t left_no, page_t* left, int64_t key, pagenum_t right_no, page_t* right) {
	int new_root_idx = buf_alloc_page(tid);
	buffer->frames[new_root_idx].is_pinned++;
	pagenum_t new_root_no = buffer->frames[new_root_idx].page_no;
	page_t* new_root = make_node();
	
	new_root->header.is_leaf = 0;
	new_root->edge[0].key = key;
	new_root->leftmost_child_no = left_no;
	new_root->edge[0].child_no = right_no;
	new_root->header.num_keys++;
	new_root->header.parent_page_no = 0;

	left->header.parent_page_no = new_root_no;
	right->header.parent_page_no = new_root_no;
	
	if(buffer->header_read[tid] == 0){
		buf_read_header(tid);
	}
	buffer->header[tid]->root_page_no = new_root_no;
	buf_write_header(tid);

	memcpy(buffer->frames[new_root_idx].page, new_root, 4096);
	buffer->frames[new_root_idx].is_dirty = 1;
	buffer->frames[new_root_idx].is_pinned--;

	free(new_root);
	return 0;
}

int start_new_tree(int tid, record_t* record) {
	page_t* root = make_leaf();
	root->header.parent_page_no = 0;
	root->header.num_keys++;
	root->right_sibling_no = 0;
	root->records[0].key = record->key;
	memcpy(root->records[0].value, record->value, 120);
	int new_root_idx = buf_alloc_page(tid);
	memcpy(buffer->frames[new_root_idx].page, root, 4096);
	buffer->frames[new_root_idx].is_dirty = 1;
	buffer->frames[new_root_idx].is_pinned++;
	
	if(buffer->header_read[tid] == 0){
		buf_read_header(tid);
	}
	buffer->header[tid]->root_page_no = buffer->frames[new_root_idx].page_no;
	buf_write_header(tid);

	buffer->frames[new_root_idx].is_pinned--;
	free(root);
	return 0;
}

int db_insert(int tid, int64_t key, char* value) {
	if(!buffer_allocated){
		return 1;
	}
	if(tid<=0||tid>10){
		return 1;
	}
	if(!id_table||id_table->id[tid]==-1){
		return 1;
	}
	if(buffer->header_read[tid] == 0){
		buf_read_header(tid);
	}
	int flag;
	char* temp = (char*)malloc(120);
	if (!db_find(tid, key, temp)) {
		free(temp);
		return 1;
	}
	free(temp);

	record_t* record = make_record(key, value);

	if (buffer->header[tid]->root_page_no == 0) {
		flag = start_new_tree(tid, record);
		free(record);
		return flag;
	}

	pagenum_t leaf_no = find_leaf(tid, key);
	page_t* leaf;
	int leaf_idx = buf_get_page(tid, leaf_no);
	leaf = buffer->frames[leaf_idx].page;

	if (leaf->header.num_keys < LEAF_ORDER - 1) {
		buffer->frames[leaf_idx].is_pinned++;
		flag = insert_into_leaf(tid, leaf_no, leaf, record);
		buffer->frames[leaf_idx].is_pinned--;
		buffer->frames[leaf_idx].is_dirty = 1;
		free(record);
		return flag;
	}

	buffer->frames[leaf_idx].is_pinned++;
	flag = insert_into_leaf_after_splitting(tid, leaf_no, leaf, record);
	buffer->frames[leaf_idx].is_pinned--;
	buffer->frames[leaf_idx].is_dirty = 1;

	return flag;
}

// DELETION.

int get_my_index(page_t* parent, pagenum_t pagenum) {

	int i;

	for (i = 0; i < parent->header.num_keys; i++) {
		if (parent->edge[i].child_no == pagenum) {
			return i;
		}
	}
	return -1;
}

void remove_entry_from_node(pagenum_t pagenum, page_t* page, int64_t key) {

	int i = 0, num_pointers;

	if(page->header.is_leaf){
		while (page->records[i].key != key) {
			i++;
		}
		for (++i; i < page->header.num_keys; i++) {
			page->records[i - 1].key = page->records[i].key;
			memcpy(page->records[i - 1].value, page->records[i].value, 120);
		}
		page->header.num_keys--;
		page->records[page->header.num_keys].key = 0;
		memset(page->records[page->header.num_keys].value, 0, 120);
	}
	else {
		while (page->edge[i].key != key) {
			i++;
		}
		for (++i; i < page->header.num_keys; i++) {
			page->edge[i - 1].key = page->edge[i].key;
			page->edge[i - 1].child_no = page->edge[i].child_no;
		}
		page->header.num_keys--;
		page->edge[page->header.num_keys].key = 0;
		page->edge[page->header.num_keys].child_no = 0;
	}
}


int adjust_root(int tid, pagenum_t root_no, page_t* root, int buffer_idx) {
	if(buffer->header_read[tid] == 0){
		buf_read_header(tid);
	}
	if (root->header.num_keys > 0) {
		return 0;
	}

	pagenum_t new_root_no = 0;
	if (!root->header.is_leaf) {
		new_root_no = root->leftmost_child_no;
		int new_root_idx = buf_get_page(tid, new_root_no);
		page_t* new_root = buffer->frames[new_root_idx].page;
		new_root->header.parent_page_no = 0;
		buffer->frames[new_root_idx].is_dirty = 1;
	}

	buffer->header[tid]->root_page_no = new_root_no;
	buf_write_header(tid);

	buf_free_page(buffer_idx);
	return 0;
}

int coalesce_nodes(int tid, pagenum_t parent_no, page_t* parent, pagenum_t my_page_no, page_t* my_page, pagenum_t neighbor_no, page_t* neighbor, int my_index, int neighbor_index){
	int64_t key;
	if (my_page->header.is_leaf) {
		if (my_index == -1) {
			my_page->records[0].key = neighbor->records[0].key;
			memcpy(my_page->records[0].value, neighbor->records[0].value, 120);
			my_page->right_sibling_no = neighbor->right_sibling_no;
			my_page->header.num_keys++;
			
			key = parent->edge[0].key;

			return delete_entry(tid, parent_no, key);
		}
		else {
			neighbor->right_sibling_no = my_page->right_sibling_no;
			key = parent->edge[my_index].key;
			
			return delete_entry(tid, parent_no, key);
		}
	}
	else {
		page_t* child;
		int child_idx;
		if (my_index == -1) {
			my_page->edge[0].key = parent->edge[0].key;
			my_page->edge[0].child_no = neighbor->leftmost_child_no;
			
			child_idx = buf_get_page(tid, my_page->edge[0].child_no);
			child = buffer->frames[child_idx].page;
			child->header.parent_page_no = my_page_no;
			buffer->frames[child_idx].is_dirty = 1;

			my_page->edge[1].key = neighbor->edge[0].key;
			my_page->edge[1].child_no = neighbor->edge[0].child_no;

			child_idx = buf_get_page(tid, my_page->edge[1].child_no);
			child = buffer->frames[child_idx].page;
			child->header.parent_page_no = my_page_no;
			buffer->frames[child_idx].is_dirty = 1;

			my_page->header.num_keys += 2;

			key = parent->edge[0].key;
			
			return delete_entry(tid, parent_no, key);
		}
		else {
			neighbor->edge[1].key = parent->edge[my_index].key;
			neighbor->edge[1].child_no = my_page->leftmost_child_no;
			child_idx = buf_get_page(tid, neighbor->edge[1].child_no);
			child = buffer->frames[child_idx].page;

			child->header.parent_page_no = neighbor_no;
			buffer->frames[child_idx].is_dirty = 1;
			neighbor->header.num_keys++;

			key = parent->edge[my_index].key;

			return delete_entry(tid, parent_no, key);
		}
	}
}

int redistribute_nodes(int tid, pagenum_t parent_no, page_t* parent, pagenum_t my_page_no, page_t* my_page, pagenum_t neighbor_no, page_t* neighbor, int my_index, int neighbor_index) {
	if (my_page->header.is_leaf) {
		if (my_index == -1) {
			my_page->records[0].key = neighbor->records[0].key;
			memcpy(my_page->records[0].value, neighbor->records[0].value, 120);
			my_page->header.num_keys++;
			for (int i = 0; i < neighbor->header.num_keys - 1; i++) {
				neighbor->records[i].key = neighbor->records[i + 1].key;
				memcpy(neighbor->records[i].value, neighbor->records[i + 1].value, 120);
			}
			neighbor->records[neighbor->header.num_keys - 1].key = 0;
			memset(neighbor->records[neighbor->header.num_keys - 1].value, 0, 120);
			neighbor->header.num_keys--;

			parent->edge[0].key = neighbor->records[0].key;
		}
		else {
			my_page->records[0].key = neighbor->records[neighbor->header.num_keys - 1].key;
			memcpy(my_page->records[0].value, neighbor->records[neighbor->header.num_keys - 1].value, 120);
			my_page->header.num_keys++;
			
			parent->edge[my_index].key = my_page->records[0].key;
			neighbor->records[neighbor->header.num_keys - 1].key = 0;
			memset(neighbor->records[neighbor->header.num_keys - 1].value, 0, 120);
			neighbor->header.num_keys--;

		}
	}
	else {
		page_t* child;
		int child_idx;
		if (my_index == -1) {
			my_page->edge[0].key = parent->edge[0].key;
			my_page->edge[0].child_no = neighbor->leftmost_child_no;
			child_idx = buf_get_page(tid, neighbor->leftmost_child_no);
			child = buffer->frames[child_idx].page;

			child->header.parent_page_no = my_page_no;
			parent->edge[0].key = neighbor->edge[0].key;
			my_page->header.num_keys++;

			neighbor->leftmost_child_no = neighbor->edge[0].child_no;
			for (int i = 0; i < neighbor->header.num_keys - 1; i++) {
				neighbor->edge[i].key = neighbor->edge[i + 1].key;
				neighbor->edge[i].child_no = neighbor->edge[i + 1].child_no;
			}
			neighbor->edge[neighbor->header.num_keys - 1].key = 0;
			neighbor->edge[neighbor->header.num_keys - 1].child_no = 0;
			neighbor->header.num_keys--;

			buffer->frames[child_idx].is_dirty = 1;

		}
		else {
			my_page->edge[0].child_no = my_page->leftmost_child_no;
			my_page->edge[0].key = parent->edge[my_index].key;
			parent->edge[my_index].key = neighbor->edge[neighbor->header.num_keys - 1].key;
			my_page->leftmost_child_no = neighbor->edge[neighbor->header.num_keys - 1].child_no;
			
			child_idx = buf_get_page(tid, my_page->leftmost_child_no);
			child = buffer->frames[child_idx].page;
			child->header.parent_page_no = my_page_no;
			my_page->header.num_keys++;

			neighbor->edge[neighbor->header.num_keys - 1].key = 0;
			neighbor->edge[neighbor->header.num_keys - 1].child_no = 0;
			neighbor->header.num_keys--;

			buffer->frames[child_idx].is_dirty = 1;

		}
	}
	return 0;
}

int delete_entry(int tid, pagenum_t pagenum, int64_t key) {

	int min_keys;
	int my_index;
	int capacity;
	int flag;
	
	if(buffer->header_read[tid] == 0){
		buf_read_header(tid);
	}
	page_t* page;
	int page_idx = buf_get_page(tid, pagenum);
	page = buffer->frames[page_idx].page;
	buffer->frames[page_idx].is_pinned++;
	remove_entry_from_node(pagenum, page, key);
	buffer->frames[page_idx].is_dirty = 1;

	if (pagenum == buffer->header[tid]->root_page_no) {
		flag = adjust_root(tid, pagenum, page, page_idx);
		buffer->frames[page_idx].is_pinned--;
		return flag;
	}

	min_keys = 1;

	if (page->header.num_keys >= min_keys) {	
		buffer->frames[page_idx].is_pinned--;
		return 0;
	}

	pagenum_t parent_no = page->header.parent_page_no;
	page_t* parent, *neighbor;

	int parent_idx = buf_get_page(tid, parent_no);
	parent = buffer->frames[parent_idx].page;
	buffer->frames[parent_idx].is_pinned++;

	my_index = get_my_index(parent, pagenum);
	int neighbor_index = (my_index == -1 ? 0 : my_index - 1);
	
	pagenum_t neighbor_no = (neighbor_index == -1 ? parent->leftmost_child_no : parent->edge[neighbor_index].child_no);

	int neighbor_idx = buf_get_page(tid, neighbor_no);
	neighbor = buffer->frames[neighbor_idx].page;
	buffer->frames[neighbor_idx].is_pinned++;

	capacity = 2;

	if (neighbor->header.num_keys < capacity) {
		flag = coalesce_nodes(tid, parent_no, parent, pagenum, page, neighbor_no, neighbor, my_index, neighbor_index);
		buffer->frames[parent_idx].is_dirty = 1;
		buffer->frames[neighbor_idx].is_dirty = 1;
		buffer->frames[page_idx].is_dirty = 1;

		buffer->frames[parent_idx].is_pinned--;
		buffer->frames[neighbor_idx].is_pinned--;
		buffer->frames[page_idx].is_pinned--;

		if (my_index == -1) {
			buf_free_page(neighbor_idx);
		}
		else {
			buf_free_page(page_idx);
		}
		return flag;
	}
	else {
		flag = redistribute_nodes(tid, parent_no, parent, pagenum, page, neighbor_no, neighbor, my_index, neighbor_index);
		buffer->frames[parent_idx].is_dirty = 1;
		buffer->frames[neighbor_idx].is_dirty = 1;
		buffer->frames[page_idx].is_dirty = 1;

		buffer->frames[parent_idx].is_pinned--;
		buffer->frames[neighbor_idx].is_pinned--;
		buffer->frames[page_idx].is_pinned--;

		return flag;
	}
}

int db_delete(int tid, int64_t key) {
	
	if(!buffer_allocated){
		return 1;
	}
	if(tid<=0||tid>10){
		return 1;
	}
	if(!id_table||id_table->id[tid] ==-1){
		return 1;
	}

	pagenum_t key_leaf_no;

	key_leaf_no = find_leaf(tid, key);
	char* temp = (char*)malloc(120);

	if (!db_find(tid, key, temp)) {
		free(temp);
		return delete_entry(tid, key_leaf_no, key);
	}
	free(temp);
	return 1;
}
