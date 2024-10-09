/*
 * An emulation of the WiredTiger eviction algorithm for the
 * in-memory cache.
 */

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"
#include "../../dataStructure/map/map.h"
#include <errno.h>

/* WiredTiger page types */
#define WT_ROOT -1
#define WT_INTERNAL 0
#define WT_LEAF 1

/* WiredTiger eviction constants */
#define WT_CACHE_OVERHEAD_PCT 8 /* the default, can be changed via connection config */

#define WT_READGEN_NOTSET 0
#define WT_READGEN_OLDEST 1
#define WT_READGEN_START_VALUE 100

#define WT_THOUSAND 1000

#define WT_CLEAR_EVICT_STATE(obj) \
	obj->wt_page.evict_bucket = -1; \
	obj->wt_page.evict_bucket_set = -1; \
	obj->wt_page.evict_obj_prev = obj->wt_page.evict_obj_next = NULL;

#define WT_EVICT_BUCKET_SET(obj) obj->wt_page.evict_bucket >= 0

typedef enum { /* WiredTiger operations on pages */
    WT_ACCESS,    /* WT accessed the page */
    WT_EVICT,     /* WT evicted the page */
    WT_EVICT_ADD, /* WT eviction added the page to an evict queue */
    WT_EVICT_LOOK /* page is accessed by evict pass, not by application */
} WT_OP_TYPE;

/* WiredTiger flags */
#define WT_READ_PREV 1
#define WT_RESTART   2
#define WT_NOTFOUND  3

#define WT_READGEN_OLDEST 1

#define WT_MAX_MEMPAGE 5*1024*1024 /* Default value for max memory page size */
#define WT_MAX_LEAFPAGE 32768 /* Default value for max leaf page size */

#define WT_EVICT_BUCKET_RANGE 100

#ifdef __cplusplus
extern "C" {
#endif

/* Public API */
static void WT_free(cache_t *cache);
static bool WT_get(cache_t *cache, const request_t *req);
static cache_obj_t *WT_find(cache_t *cache, const request_t *req,
                             const bool update_cache);
static cache_obj_t *WT_insert(cache_t *cache, const request_t *req);
static cache_obj_t *WT_to_evict(cache_t *cache, const request_t *req);
static void WT_evict(cache_t *cache, const request_t *req);
static bool WT_remove(cache_t *cache, const obj_id_t obj_id);
static void WT_print_cache(const cache_t *cache);

/* Internal functions */

/* BTree management */
typedef void (*BTreeInquiryFunction)(const cache_t *, cache_obj_t *, void *);

static cache_obj_t *__btree_find_parent(cache_obj_t *start, obj_id_t obj_id);
static int __btree_init_page(const cache_t *cache, cache_obj_t *obj, short page_type,
                             cache_obj_t *parent_obj, int read_gen);
static void __btree_node_index_slot(cache_obj_t *obj, Map *children, int *slotp);
static inline cache_obj_t * __btree_node_parent(cache_obj_t *node);
static char * __btree_page_to_string(cache_obj_t *obj);
static void __btree_print(const cache_t *cache);
static void __btree_remove(const cache_t *cache, cache_obj_t *obj);
static void __btree_walk_compute(const cache_t *cache, cache_obj_t *curNode, BTreeInquiryFunction func, void* argp);
static void __btree_tree_walk_count(const cache_t *cache, cache_obj_t **nodep, int *walkcntp,
                                   int walk_flags);
static void __btree_min_readgen(const cache_t *cache, cache_obj_t *obj, void *ret_arg);
static void __btree_min_readgen_leaf(const cache_t *cache, cache_obj_t *obj, void *ret_arg);

#define FLAG_SET(memory, value) (memory |= value)
#define FLAG_ISSET(memory, value) ((memory & value) != 0)
#define FLAG_CLEAR(memory, value) (((memory) &= ~(value)))

/* Eviction */
static void	__evict_update_obj_read_gen(const cache_t *cache, cache_obj_t *cache_obj, uint64_t readgen);
static void __add_to_evict_bucket(const cache_t *cache, cache_obj_t *obj);
static void __remove_from_evict_bucket(const cache_t *cache, cache_obj_t *obj);
static void	__renumber_evict_buckets(const cache_t *cache, int bucket_set);
static void __evict_print_bucket_set(const cache_t *cache, int bucket_set);

/* Other general functions */
static char* __op_to_string(int op);

/**
 * @brief initialize a WiredTiger cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params WiredTiger specific parameters, should be NULL
 */
cache_t *WT_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params) {
    cache_t *cache = cache_struct_init("WiredTiger", ccache_params, cache_specific_params);
    cache->cache_init = WT_init;
    cache->cache_free = WT_free;
    cache->get = WT_get;
    cache->find = WT_find;
    cache->insert = WT_insert;
    cache->evict = WT_evict;
    cache->remove = WT_remove;
    cache->to_evict = WT_to_evict;
    cache->get_occupied_byte = cache_get_occupied_byte_default;
    cache->can_insert = cache_can_insert_default;
    cache->get_n_obj = cache_get_n_obj_default;
    cache->print_cache = WT_print_cache;

    if (ccache_params.consider_obj_metadata) {
        cache->obj_md_size = 8 * 2;
    } else {
        cache->obj_md_size = 0;
    }

    WT_params_t *params = (WT_params_t *)malloc(sizeof(WT_params_t));
    memset(params, 0, sizeof(WT_params_t));

	/*
	 * Initialize the upper bounds of read generations for evict buckets.
	 */
	for (int i = 0; i < WT_EVICT_BUCKET_SETS; i++) {
		for (int j = 0; j < WT_NUM_EVICT_BUCKETS; j++) {
			params->evict_buckets[i][j].upper_bound = (j+1) * 100;
		}
	}

    params->cache_size = ccache_params.cache_size;
    params->splitmempage = 8 * MAX(WT_MAX_MEMPAGE, WT_MAX_LEAFPAGE) / 10;
    cache->eviction_params = params;
    srand(time(NULL));

    return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void WT_free(cache_t *cache)
{
    /* TODO -- Walk the tree and free all objects */
    cache_struct_free(cache);
}

/**
 * @brief this function is the user facing API
 * it performs the following logic
 *
 * ```
 * if obj in cache:
 *    update_metadata
 *    return true
 * else:
 *    if cache does not have enough space:
 *        evict until it has space to insert
 *    insert the object
 *    return false
 * ```
 *
 * @param cache
 * @param req
 * @return true if cache hit, false if cache miss
 */
static bool WT_get(cache_t *cache, const request_t *req)
{
    return cache_get_base(cache, req);
}

// ***********************************************************************
// ****                                                               ****
// ****       developer facing APIs (used by cache developer)         ****
// ****                                                               ****
// ***********************************************************************

/**
 * @brief check whether an object is in the cache
 *
 * @param cache
 * @param req
 * @param update_cache whether to update the cache,
 *  if true, the object is promoted
 *  and if the object is expired, it is removed from the cache
 * @return true on hit, false on miss
 */
static cache_obj_t *WT_find(cache_t *cache, const request_t *req,
                             const bool update_cache) {
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    cache_obj_t *cache_obj = cache_find_base(cache, req, update_cache);

#if 0
	INFO("%ld,%ld,%ld,%ld,%d,%d,%d\n", req->clock_time, req->obj_id, req->obj_size,
		   req->parent_addr, req->page_type, req->read_gen, req->operation_type);
#endif
    if (cache_obj != NULL) {
        if (!cache_obj->wt_page.in_tree || cache_obj->wt_page.page_type != req->page_type
            || cache_obj->wt_page.parent_page->obj_id != req->parent_addr) {
            ERROR("Cached WiredTiger object changed essential properties.\n");
        }
        else if (cache_obj->wt_page.read_gen != req->read_gen)
			__evict_update_obj_read_gen(cache, cache_obj, req->read_gen);
    }

  done:
    if (req->operation_type != WT_ACCESS) {
        WARN("Operation NOT access\n");
        cache_obj = (cache_obj_t *) 0xDEADBEEF;
    }
    return cache_obj;
}

/**
 * @brief insert an object into the cache,
 * update the hash table and cache metadata
 * this function assumes the cache has enough space
 * and eviction is not part of this function
 *
 * @param cache
 * @param req
 * @return the inserted object
 */
static cache_obj_t *WT_insert(cache_t *cache, const request_t *req) {
    cache_obj_t *obj, *parent_page;
    WT_params_t *params = (WT_params_t *)cache->eviction_params;

    obj = cache_insert_base(cache, req);


    INFO("WT_insert: addr = %ld, parent_addr = %ld, read_gen = %d, type = %d, op = %d\n",
         req->obj_id, req->parent_addr, req->read_gen, req->page_type, req->operation_type);

    if (params->BTree_root == NULL) {
        /*
         * B-Tree is not initialized. If we are processing the first record in the
         * trace, we expect that record to identify the root node. If we see the
         * record that does not identify the root note and the BTree is not initialized,
         * this is an error.
         */
        if (req->parent_addr == 0) { /* Root is the only node with parent address zero. */
            params->BTree_root = obj;
            if (__btree_init_page(cache, obj, WT_ROOT, NULL /* parent */, 0 /* read gen */) != 0)
                return NULL;
            return obj;
        } else {
            ERROR("WiredTiger BTree has not been initialized\n");
                return NULL;
        }
    }

    if (obj->wt_page.in_tree) {
        ERROR("New WiredTiger object already in tree\n");
        return NULL;
    }

    if (req->parent_addr == 0) {
        ERROR("New root with root already present. Existing root address is %ld,"
              "this root access has address %ld. We do not support more than one tree.\n",
               params->BTree_root->obj_id, obj->obj_id);
        return NULL;
    }

    /* The object is not root and is not in tree. Insert it under its parent. */
    if((parent_page = __btree_find_parent(params->BTree_root, req->parent_addr)) == NULL) {
        ERROR("Parent %ld of WiredTiger object %ld not found in WiredTiger tree. Cannot insert.\n",
              req->parent_addr, obj->obj_id);
        return NULL;
    } else {
        DEBUG_ASSERT(parent_page->obj_id == req->parent_addr);

        if (__btree_init_page(cache, obj, req->page_type, parent_page, req->read_gen) != 0)
            return NULL;
        if (insertPair(parent_page->wt_page.children, obj->obj_id, (void*)obj) != 0) {
            ERROR("WiredTiger could not insert a child into the parent's map\n");
            deleteMap(obj->wt_page.children);
            return NULL;
        }
    }
    return obj;
}

/**
 * @brief find the object to be evicted
 * this function does not actually evict the object or update metadata
 * not all eviction algorithms support this function
 * because the eviction logic cannot be decoupled from finding eviction
 * candidate, so use assert(false) if you cannot support this function
 *
 * @param cache
 * @return the object to be evicted
 */
static cache_obj_t *WT_to_evict(cache_t *cache, const request_t *req) {
  WT_params_t *params = (WT_params_t *)cache->eviction_params;

  WARN("WT_to_evict: \n");
  DEBUG_ASSERT(false);
  return NULL;
}

/**
 * @brief evict an object from the cache
 * it needs to call cache_evict_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param req not used
 */
static void WT_evict(cache_t *cache, const request_t *req) {
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
	wt_evict_bucket_t *bucket_set;
	cache_obj_t *evict_victim;
	bool using_internal_bucket_set = false;

//#define IDEAL_EVICT
#ifdef IDEAL_EVICT
	/*
	 * An idealized __evict function, where we find the leaf page with the smallest
	 * read generation to evict.
	 */
	cache_obj_t *min_readgen_leafobj = NULL;
	__btree_walk_compute(cache, NULL, __btree_min_readgen_leaf, &min_readgen_leafobj);

	if (min_readgen_leafobj == NULL)
		ERROR("Ideal eviction: nothing to evict\n");
	WARN("Ideal evicted: %s. %ld bytes cached\n", __btree_page_to_string(min_readgen_leafobj),
		 params->cache_inmem_bytes);
	__btree_remove(cache, min_readgen_leafobj);
	evicted_since_last_fill++;

#else
	INFO("Before eviction: Leaf bucket set\n");
	__evict_print_bucket_set(cache, WT_EVICT_BUCKET_SET_LEAF);
	/*
	 * First look for candidates in the bucket set for leaf pages.
	 * If nothing there (unlikely) look through the internal page bucket set.
	 */
	bucket_set = params->evict_buckets[WT_EVICT_BUCKET_SET_LEAF];
	INFO("EVICT: search in leaf bucket set\n");
  retry:
	for (int i = 0; i < WT_NUM_EVICT_BUCKETS; i++) {
		/* LOCK QUEUE */
		if ((evict_victim = bucket_set[i].evict_queue.head) != NULL) {
			INFO("evict victim found in bucket %d, upper range %d\n", i, bucket_set[i].upper_bound);
			bucket_set[i].evict_queue.head = evict_victim->wt_page.evict_obj_next;
			if (evict_victim->wt_page.evict_obj_next != NULL)
				evict_victim->wt_page.evict_obj_next->wt_page.evict_obj_prev = NULL;
			else {
				DEBUG_ASSERT(bucket_set[i].evict_queue.tail == evict_victim);
				bucket_set[i].evict_queue.tail = NULL;
			}
		}
		/* UNLOCK QUEUE */
		if (evict_victim != NULL)
			break;
	}
	if (evict_victim == NULL) {
		if (using_internal_bucket_set) {
			ERROR("Could not find anyone to evict\n");
		}
		else {
			using_internal_bucket_set = true;
			bucket_set = params->evict_buckets[WT_EVICT_BUCKET_SET_INTERNAL];
			WARN("EVICT: search internal bucket set\n");
			goto retry;
		}
	}
	WT_CLEAR_EVICT_STATE(evict_victim);
	WARN("evicted: %s. %ld bytes cached\n", __btree_page_to_string(evict_victim),
		 params->cache_inmem_bytes);
	__btree_remove(cache, evict_victim);

	/* Report the smallest read generation among the cached objects */
	cache_obj_t *min_readgen_obj = NULL;
	__btree_walk_compute(cache, NULL, __btree_min_readgen, &min_readgen_obj);
	INFO("Smallest read generation: %s\n", __btree_page_to_string(min_readgen_obj));
#endif
}

/**
 * @brief remove an object from the cache
 * this is different from cache_evict because it is used to for user trigger
 * remove, and eviction is used by the cache to make space for new objects
 *
 * it needs to call cache_remove_obj_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param obj_id
 * @return true if the object is removed, false if the object is not in the
 * cache
 */
static bool WT_remove(cache_t *cache, const obj_id_t obj_id) {
    cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);

    INFO("WT_remove: \n");
    if (obj == NULL) {
        return false;
    }
    __btree_remove(cache, obj);
    return true;
}

static void WT_print_cache(const cache_t *cache) {
    __btree_print(cache);
}

static inline uint64_t
__btree_bytes_evictable(WT_params_t *params) {
    return (params->btree_inmem_bytes - params->BTree_root->obj_size)
        * (100 + WT_CACHE_OVERHEAD_PCT) / 100;
}

static inline uint64_t
__btree_cache_bytes_inuse(WT_params_t *params) {
    return params->cache_inmem_bytes + (params->cache_inmem_bytes * WT_CACHE_OVERHEAD_PCT) / 100;
}

static inline uint64_t
__cache_pages_inuse(WT_params_t *params) {
    return params->pages_inmem;
}

static cache_obj_t *
__btree_find_parent(cache_obj_t *start, obj_id_t parent_id) {
    Map children;
    cache_obj_t *found_node, *child_node;
    size_t i;

    if (parent_id == start->obj_id)
        return start;
    if ((children = start->wt_page.children) == NULL)
        return NULL;
    if ((found_node = (cache_obj_t*)getValue(children, parent_id)) != NULL)
        return found_node;

    for (i = 0; i < getMapSize(children); i++) {
        child_node = getValueAtIndex(children, i);
        DEBUG_ASSERT(child_node != NULL);
        if ((found_node = __btree_find_parent(child_node, parent_id)) != NULL)
            return found_node;
    }
    return NULL;
}

static inline cache_obj_t *
__btree_node_parent(cache_obj_t *node) {
    return node->wt_page.parent_page;
}

/*
 * Find the node's slot id in its parent array.
 */
static void
__btree_node_index_slot(cache_obj_t *node, Map *children, int *slotp) {
    cache_obj_t *parent;

    DEBUG_ASSERT(node != NULL && node->wt_page.page_type != WT_ROOT);

    parent = node->wt_page.parent_page;
    *children = parent->wt_page.children;

    for (*slotp = 0; *slotp < getMapSize(*children); (*slotp)++)
        if (node == getValueAtIndex(*children, *slotp))
            break;

    DEBUG_ASSERT(*slotp < getMapSize(*children));
}

static int
__btree_init_page(const cache_t *cache, cache_obj_t *obj, short page_type,
                  cache_obj_t *parent_page, int read_gen) {
	WT_params_t *params = (WT_params_t *)cache->eviction_params;
    obj->wt_page.page_type = page_type;
    obj->wt_page.parent_page = parent_page;
    obj->wt_page.read_gen = read_gen;
    obj->wt_page.in_tree = true;

    if (page_type != WT_LEAF)
        if ((obj->wt_page.children = createMap()) == NULL)
            return ENOMEM;

    /*
     * These are the same as long as we are supporting only a single B-Tree, but keep track of
     * them separately in case we implement the multi-B-tree functionality.
     */
    params->cache_inmem_bytes += obj->obj_size;
    params->btree_inmem_bytes += obj->obj_size;
    params->pages_inmem++;
    if (read_gen != WT_READGEN_NOTSET) {
        if (params->read_gen_oldest == WT_READGEN_NOTSET ||
            read_gen < params->read_gen_oldest)
            params->read_gen_oldest = read_gen;
    }
    if (read_gen > params->read_gen)
        params->read_gen = read_gen;

	WT_CLEAR_EVICT_STATE(obj);
	/*
	 * We don't add the root to the evict queue. We don't add pages whose read generation is not
	 * set. They will be added once the read generation is set.
	 */
	if (page_type != WT_ROOT && obj->wt_page.read_gen != WT_READGEN_NOTSET)
		__add_to_evict_bucket(cache, obj);

    if (page_type != WT_LEAF)
        params->btree_internal_pages++;

#if 0
	INFO("After inserting new page\n");
	__evict_print_bucket_set(cache,
							 (obj->wt_page.page_type == WT_INTERNAL) ? WT_EVICT_BUCKET_SET_INTERNAL : WT_EVICT_BUCKET_SET_LEAF);
#endif
    return 0;
}

static inline void
__btree_print_node_nochildren(cache_obj_t *node) {
    printf("%s", __btree_page_to_string(node));
}

static void
__btree_print_node(cache_obj_t *node, int times)
{
    int k;
    cache_obj_t *child_node;
    size_t i;

    if (node == NULL) return;

    __btree_print_node_nochildren(node);

    if (node->wt_page.page_type != WT_LEAF) {
        for (i = 0; i < getMapSize(node->wt_page.children); i++) {
            child_node = getValueAtIndex(node->wt_page.children, i);
            DEBUG_ASSERT(child_node != NULL);
            printf("\n");
            for (k = 0; k < times; k++) printf("  ");
            __btree_print_node(child_node, times + 1);
        }
    }
}

static void
__btree_print(const cache_t *cache){
    WT_params_t *params = (WT_params_t *)cache->eviction_params;

    printf("BEGIN ----------------------------------------\n");
    __btree_print_node(params->BTree_root, 0);
    printf("\nEND ----------------------------------------\n");

}

/*
 * Recursively remove the object's children from the B-Tree, and remove itself from its parent.
 */
static void
__btree_remove(const cache_t *cache, cache_obj_t *obj) {
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    int i;

    DEBUG_ASSERT(obj != NULL);

    if (obj->wt_page.page_type == WT_ROOT) {
        ERROR("WiredTiger: not allowed to remove root\n");
    }
    else if (obj->wt_page.page_type == WT_INTERNAL) {
        if (getMapSize(obj->wt_page.children) > 0)
            WARN("Removing internal page with children: %s\n", __btree_page_to_string(obj));
        for (i = 0; i < getMapSize(obj->wt_page.children); i++)
            __btree_remove(cache, getValueAtIndex(obj->wt_page.children, i));
        deleteMap(obj->wt_page.children);
    }

	if (WT_EVICT_BUCKET_SET(obj))
		__remove_from_evict_bucket(cache, obj);
#if 0
	INFO("After removing page\n");
	__evict_print_bucket_set(cache,
							 (obj->wt_page.page_type == WT_INTERNAL) ? WT_EVICT_BUCKET_SET_INTERNAL : WT_EVICT_BUCKET_SET_LEAF);
#endif
    DEBUG_ASSERT(removePair(obj->wt_page.parent_page->wt_page.children, obj->obj_id));
    INFO("Removed object %lu from parent %lu (map %p)\n", obj->obj_id,
         obj->wt_page.parent_page->obj_id,
         obj->wt_page.parent_page->wt_page.children);
    params->pages_evicted++;
    params->pages_inmem--;
    params->cache_inmem_bytes -= obj->obj_size;
    params->btree_inmem_bytes -= obj->obj_size;

    if (obj->wt_page.page_type == WT_INTERNAL)
        params->btree_internal_pages--;

    cache_evict_base((cache_t *)cache, obj, true);
}

static void
__btree_walk_compute(const cache_t *cache, cache_obj_t *curNode,
					 BTreeInquiryFunction func, void* argp) {
	WT_params_t *params = (WT_params_t *)cache->eviction_params;

	if (curNode == NULL) { /* Begin walking from the root */
		if (params->BTree_root == NULL)
			WARN("BTree root null. Can't walk tree\n");
		else
			__btree_walk_compute(cache, params->BTree_root, func, argp);
	}
	else {
		if (curNode->wt_page.page_type != WT_LEAF) {
			if (getMapSize(curNode->wt_page.children) > 0) {
				for (int i = 0; i < getMapSize(curNode->wt_page.children); i++)
					__btree_walk_compute(cache,  getValueAtIndex(curNode->wt_page.children, i),
										 func, argp);
			}
		}
		func(cache, curNode, argp);
	}
}

/*
 * We use this global buffer for pretty-printing the page. We are single-threaded, so it's okay
 * to use this global buffer. We do this, so that we don't have to dynamically allocate the memory
 * and the caller doesn't need to free it for us.
 */
#define PAGE_PRINT_BUFFER_SIZE 100
char page_buffer[PAGE_PRINT_BUFFER_SIZE];

static char *
__btree_page_to_string(cache_obj_t *obj) {

    snprintf((char*)&page_buffer, PAGE_PRINT_BUFFER_SIZE,
             "page %lu [%lu], %s, read-gen: %ld, evict-bucket-set: %d, evict-bucket: %d", obj->obj_id,
             (obj->wt_page.parent_page == NULL)?0:obj->wt_page.parent_page->obj_id,
             (obj->wt_page.page_type == WT_LEAF)?"leaf":(obj->wt_page.page_type == WT_INTERNAL)?"int":"root",
             obj->wt_page.read_gen, obj->wt_page.evict_bucket_set, obj->wt_page.evict_bucket);
    return (char *) page_buffer;
}

static char* __op_to_string(int op) {
    switch(op) {
    case  WT_ACCESS:
        return "access";
    case  WT_EVICT:
        return "evict";
    case WT_EVICT_ADD:
        return "evict_add";
    case WT_EVICT_LOOK:
        return "evict_look";
    }
}

static void __btree_min_readgen(const cache_t *cache, cache_obj_t *obj, void *ret_arg) {
	cache_obj_t *min_readgen_obj = *(cache_obj_t**)ret_arg;

	if (min_readgen_obj == NULL ||
		min_readgen_obj->wt_page.read_gen == WT_READGEN_NOTSET ||
		(obj->wt_page.read_gen != WT_READGEN_NOTSET &&
		 obj->wt_page.read_gen < min_readgen_obj->wt_page.read_gen)) {
		*(cache_obj_t**)ret_arg = obj;
	}
}

static void __btree_min_readgen_leaf(const cache_t *cache, cache_obj_t *obj, void *ret_arg) {
	cache_obj_t *min_readgen_obj = *(cache_obj_t**)ret_arg;

	if (obj->wt_page.page_type != WT_LEAF)
		return;

	if ((min_readgen_obj == NULL || min_readgen_obj->wt_page.read_gen == WT_READGEN_NOTSET)
		|| (obj->wt_page.read_gen != WT_READGEN_NOTSET &&
			obj->wt_page.read_gen < min_readgen_obj->wt_page.read_gen))
		*(cache_obj_t**)ret_arg = obj;
}

static void	__evict_update_obj_read_gen(const cache_t *cache, cache_obj_t *obj, uint64_t read_gen) {
	WT_params_t *params = (WT_params_t *)cache->eviction_params;
	int my_bucket, my_bucket_set;

	INFO("Update read generation from %ld to %ld: %s\n", obj->wt_page.read_gen, read_gen, __btree_page_to_string(obj));

	my_bucket = obj->wt_page.evict_bucket;
	my_bucket_set = obj->wt_page.evict_bucket_set;
	obj->wt_page.read_gen = read_gen;

	/*
	 * If the object is in the right bucket, our work is done.
	 * We check this without locking. If the bucket range is currently being changed,
	 * we will make the wrong decision, which is okay, because bucket ranges don't
	 * change dramatically, and we tolerate some degree of inaccuracy in bucket placement.
	 */
	if (WT_EVICT_BUCKET_SET(obj)) {
		if (obj->wt_page.read_gen < params->evict_buckets[my_bucket_set][my_bucket].upper_bound &&
			obj->wt_page.read_gen >=
			(params->evict_buckets[my_bucket_set][my_bucket].upper_bound - WT_EVICT_BUCKET_RANGE))
			return;
		/* Otherwise, remove from the current bucket and add to the new bucket */
		__remove_from_evict_bucket(cache, obj);
	}
	__add_to_evict_bucket(cache, obj);
#if 0
	INFO("New bucket is: %d (upper range = %d)\n",
		 obj->wt_page.evict_bucket, params->evict_buckets[my_bucket_set][obj->wt_page.evict_bucket].upper_bound);

	INFO("After read generation update\n");
	__evict_print_bucket_set(cache,
							 (obj->wt_page.page_type == WT_INTERNAL) ? WT_EVICT_BUCKET_SET_INTERNAL : WT_EVICT_BUCKET_SET_LEAF);
#endif
}

static void __remove_from_evict_bucket(const cache_t *cache, cache_obj_t *obj) {
	WT_params_t *params = (WT_params_t *)cache->eviction_params;
	int my_bucket, my_bucket_set;
	wt_evict_queue_t *queue;

	if (obj->wt_page.page_type == WT_ROOT)
		return;

	my_bucket = obj->wt_page.evict_bucket;
	my_bucket_set = obj->wt_page.evict_bucket_set;
	queue = &params->evict_buckets[my_bucket_set][my_bucket].evict_queue;

	INFO("Remove from bucket set %d, bucket %d %s\n", my_bucket_set, my_bucket, __btree_page_to_string(obj));

	/* LOCK QUEUE */
	if (queue->head == obj)
		queue->head = obj->wt_page.evict_obj_next;
	else { /* If our object is not the head, the pointer to the previous object must be valid */
		obj->wt_page.evict_obj_prev->wt_page.evict_obj_next = obj->wt_page.evict_obj_next;
	}
	if (queue->tail == obj) {
		queue->tail = obj->wt_page.evict_obj_prev;
	}
	else  /* If our object is not the tail, the pointer to the next object must be valid */
		obj->wt_page.evict_obj_next->wt_page.evict_obj_prev = obj->wt_page.evict_obj_prev;

	/* UNLOCK QUEUE */

	obj->wt_page.evict_obj_next = obj->wt_page.evict_obj_prev = NULL;
	WT_CLEAR_EVICT_STATE(obj);
}

static void __add_to_evict_bucket(const cache_t *cache, cache_obj_t *obj) {
	WT_params_t *params = (WT_params_t *)cache->eviction_params;
	wt_evict_queue_t *queue;
	int bucket, bucket_set;

	if (obj->wt_page.page_type == WT_ROOT)
		return;

	if (obj->wt_page.page_type == WT_LEAF)
		bucket_set = WT_EVICT_BUCKET_SET_LEAF;
	else
		bucket_set = WT_EVICT_BUCKET_SET_INTERNAL;

	obj->wt_page.evict_bucket_set = bucket_set;
/*	INFO("Add to evict bucket %s\n", __btree_page_to_string(obj)); */

  retry:
	/* Find the right bucket for the object */
	if (obj->wt_page.read_gen < params->evict_buckets[bucket_set][0].upper_bound)
		bucket = 0;
	else if (obj->wt_page.read_gen >
			 params->evict_buckets[bucket_set][WT_NUM_EVICT_BUCKETS-1].upper_bound) {
		__renumber_evict_buckets(cache, bucket_set);
		goto retry;
	}
	else {
		bucket = obj->wt_page.read_gen / WT_EVICT_BUCKET_RANGE;
		/*
		 * Our bucket may be out of range if we computed the bucket number while someone
		 * else were updating the zero-th bucket upper bound. In that case, place the object
		 * into the last bucket.
		 */
		if (bucket > WT_NUM_EVICT_BUCKETS - 1) {
			ERROR("Bucket out of range"); /* Should never happen in a single threaded simulation */
		}
	}
	obj->wt_page.evict_bucket = bucket;

	queue = &params->evict_buckets[bucket_set][bucket].evict_queue;
	/* LOCK QUEUE */
	if (queue->head == NULL) {
		DEBUG_ASSERT(queue->tail == NULL);
		queue->head = queue->tail = obj;
	}
	else {
		queue->tail->wt_page.evict_obj_next = obj;
		obj->wt_page.evict_obj_prev = queue->tail;
		queue->tail = obj;
	}
	/* UNLOCK QUEUE */
}

/*
 * If the object's read generation exceeds the upper range of the largest bucket we
 * increase the upper range of each bucket. We move the queue of each bucket one level
 * down.
 */
static void	__renumber_evict_buckets(const cache_t *cache, int bucket_set) {

	WT_params_t *params = (WT_params_t *)cache->eviction_params;
	wt_evict_queue_t *queue, *lower_queue;

	INFO("Renumber evict buckets for bucket set %d\n", bucket_set);
	INFO("Before renumbering\n");
	__evict_print_bucket_set(cache, bucket_set);

	params->evict_buckets[bucket_set][0].upper_bound += WT_EVICT_BUCKET_RANGE;

	for (int i = 1; i < WT_NUM_EVICT_BUCKETS; i++) {
		params->evict_buckets[bucket_set][i].upper_bound += WT_EVICT_BUCKET_RANGE;
		queue = &params->evict_buckets[bucket_set][i].evict_queue;
		lower_queue = &params->evict_buckets[bucket_set][i-1].evict_queue;

		INFO("Bucket %d's new upper bound is %d\n", i, params->evict_buckets[bucket_set][i].upper_bound);
		/* LOCK LOWER QUEUE */
		/* LOCK UPPER QUEUE */
		if (queue->head == NULL)
			goto unlock;

		/* Move our current queue into the lower queue */
		if (lower_queue->head == NULL) {
			lower_queue->head = queue->head;
			lower_queue->tail = queue->tail;
		}
		else {
			lower_queue->tail->wt_page.evict_obj_next = queue->head;
			queue->head->wt_page.evict_obj_prev = lower_queue->tail;
			lower_queue->tail = queue->tail;
		}
		queue->head = NULL;
		queue->tail = NULL;
	  unlock:
		/* UNLOCK LOWER QUEUE */
		/* UNLOCK UPPER QUEUE */
	}
	INFO("After renumbering\n");
	__evict_print_bucket_set(cache, bucket_set);
}

static void __evict_print_queue(wt_evict_queue_t *evict_queue) {
	cache_obj_t *cur;

	printf("Head = %p, tail = %p\n", evict_queue->head, evict_queue->tail);

	cur = evict_queue->head;
	while(cur != NULL) {
		printf("cur = %p, prev = %p, next = %p\n", cur, cur->wt_page.evict_obj_prev, cur->wt_page.evict_obj_next);
		printf("\t %s\n", __btree_page_to_string(cur));
		cur = cur->wt_page.evict_obj_next;
	}
}

static void __evict_print_bucket_set(const cache_t *cache, int bucket_set) {
	WT_params_t *params = (WT_params_t *)cache->eviction_params;

	printf("+++++ EVICT BUCKET SET %d [%s]\n", bucket_set, bucket_set==WT_EVICT_BUCKET_SET_INTERNAL?"internal":"leaf");

	for (int i = 0; i < WT_NUM_EVICT_BUCKETS; i++) {
		if (params->evict_buckets[bucket_set][i].evict_queue.head != NULL) {
			printf("======= Bucket %d, upper bound = %d ==========\n", i, params->evict_buckets[bucket_set][i].upper_bound);
			__evict_print_queue(&params->evict_buckets[bucket_set][i].evict_queue);
		}
	}
}

#ifdef __cplusplus
}
#endif
