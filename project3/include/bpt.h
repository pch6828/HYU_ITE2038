#ifndef __BPT_H__
#define __BPT_H__
// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include "file_manager.h"
#include "buffer_manager.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif
// Default order is 4.

#define LEAF_ORDER 32
#define INTERNAL_ORDER 249

//#define LEAF_ORDER 4
//#define INTERNAL_ORDER 4

// Constants for printing part or all of the GPL license.
#define LICENSE_FILE "LICENSE.txt"
#define LICENSE_WARRANTEE 0
#define LICENSE_WARRANTEE_START 592
#define LICENSE_WARRANTEE_END 624
#define LICENSE_CONDITIONS 1
#define LICENSE_CONDITIONS_START 70
#define LICENSE_CONDITIONS_END 625
typedef struct Node* PtrToNode;
typedef PtrToNode Queue;
struct Node {
	pagenum_t id;
	Queue next;
};
// GLOBALS.
/* The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
extern int order;
extern int internal_order;
/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
Queue queue;
/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
// FUNCTION PROTOTYPES.
// Output and utility.

Queue make_new_queue();/**/
int is_empty(Queue queue);/**/
void enqueue(pagenum_t pagenum, Queue queue);/**/
pagenum_t dequeue(Queue queue);/**/
int height(int tid);/**/
int path_to_root(int tid, pagenum_t pagenum);/**/
void print_leaves(int tid);/**/
void print_tree(int tid);/**/
pagenum_t find_leaf(int tid, int64_t key);/**/
int db_find(int tid, int64_t key, char* dest);/**/
int cut(int length);/**/
// Insertion.
record_t* make_record(int64_t key, char* value);/**/
page_t* make_node(void);/**/
page_t* make_leaf(void);/**/
int get_left_index(page_t* parent, pagenum_t left_no);/**/
int insert_into_leaf(int tid, pagenum_t leaf_no, page_t* leaf, record_t* record);/**/
int insert_into_leaf_after_splitting(int tid, pagenum_t leaf_no, page_t* leaf, record_t* record);/**/
int insert_into_node(int tid, pagenum_t parent_no, page_t* parent, int left_index, int64_t key, pagenum_t right_no, page_t* right);/**/
int insert_into_node_after_splitting(int tid, pagenum_t parent_no, page_t* parent, int left_index, int64_t key, pagenum_t right_no, page_t* right);/**/
int insert_into_parent(int tid, pagenum_t parent_no, pagenum_t left_no, page_t* left, int64_t key, pagenum_t right_no, page_t* right);/**/
int insert_into_new_root(int tid, pagenum_t left_no, page_t* left, int64_t key, pagenum_t right_no, page_t* right);/**/
int start_new_tree(int tid, record_t* record);/**/
int db_insert(int tid, int64_t key, char* value);/**/
// Deletion.
int get_my_index( page_t* parent, pagenum_t pagenum );//
void remove_entry_from_node(pagenum_t pagenum, page_t* page, int64_t key);//
int adjust_root(int tid, pagenum_t root_no, page_t* root, int buffer_idx);//
int coalesce_nodes(int tid, pagenum_t parent_no, page_t* parent,pagenum_t my_page_no, page_t* my_page, pagenum_t neighbor_no, page_t* neighbor, int my_index, int neighbor_index);//
int redistribute_nodes(int tid, pagenum_t parent_no, page_t* parent, pagenum_t my_page_no, page_t* my_page, pagenum_t neighbor_no, page_t* neighbor, int my_index, int neighbor_index);//
int delete_entry(int tid, pagenum_t pagenum, int64_t key);//
int db_delete(int tid, int64_t key);//

#endif /* __BPT_H__*/
