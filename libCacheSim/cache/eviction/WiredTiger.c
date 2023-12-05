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
#define WT_EVICT_WALK_BASE 300
#define WT_EVICT_WALK_INCR 100

#define WT_EVICT_QUEUE_MAX 3

#ifdef __cplusplus
extern "C" {
#endif

static void WT_free(cache_t *cache);
static bool WT_get(cache_t *cache, const request_t *req);
static cache_obj_t *WT_find(cache_t *cache, const request_t *req,
                             const bool update_cache);
static cache_obj_t *WT_insert(cache_t *cache, const request_t *req);
static cache_obj_t *WT_to_evict(cache_t *cache, const request_t *req);
static void WT_evict(cache_t *cache, const request_t *req);
static bool WT_remove(cache_t *cache, const obj_id_t obj_id);
static void WT_print_cache(const cache_t *cache);

static int __btree_evict_target();
static cache_obj_t *__btree_evict_walk_next(cache_obj_t *start);
static cache_obj_t *__btree_find_parent(cache_obj_t *start, obj_id_t obj_id);
static int __btree_init_page(cache_obj_t *obj, WT_params_t *params, short page_type, cache_obj_t *parent_obj, int read_gen);
static void __btree_print(cache_t *cache);
static void __btree_remove(cache_t *cache, cache_obj_t *obj);


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
    params->q_head = NULL;
    params->q_tail = NULL;
    if ((params->evict_q = malloc(sizeof(cache_obj_t*) * (WT_EVICT_WALK_BASE + WT_EVICT_WALK_INCR)) == NULL)
        return NULL;
    cache->eviction_params = params;

    return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void WT_free(cache_t *cache)
{
    /* XXX -- Walk the tree and free all objects */
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
    INFO("WT_get: \n");
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

    INFO("WT_find: addr = %ld, parent_addr = %ld, read_gen = %d, type = %d\n",
           req->obj_id, req->parent_addr, req->read_gen, req->page_type);

    if (cache_obj != NULL && !cache_obj->wt_page.in_tree)
        ERROR("Cached WiredTiger object not in tree\n");

    __btree_print(cache);

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


    INFO("WT_insert: addr = %ld, parent_addr = %ld, read_gen = %d, type = %d\n",
           req->obj_id, req->parent_addr, req->read_gen, req->page_type);

    if (params->BTree_root == NULL) {
        /*
         * B-Tree is not initialized. If we are processing the first record in the
         * trace, we expect that record to identify the root node. If we see the
         * record that does not identify the root note and the BTree is not initialized,
         * this is an error.
         */
        if (req->parent_addr == 0) { /* Root is the only node with parent address zero. */
            params->BTree_root = obj;
            if (__btree_init_page(obj, params, WT_ROOT, NULL /* parent page */, 0 /* read gen */) != 0)
                return NULL;
            return obj;
        } else {
            ERROR("WiredTiger BTree has not been initialized\n");
                return NULL;
        }
    }

    /* We don't do this for the root, because we never want to evict root */
    prepend_obj_to_head(&params->q_head, &params->q_tail, obj);

    if (obj->wt_page.in_tree) {
        printf("Error: new WiredTiger object already in tree\n");
        return NULL;
    }

    if (req->parent_addr == 0) {
        ERROR("Saw a new WiredTiger root record with root already present. Existing root address is %ld,"
               "this root access has address %ld. We do not support more than one tree.\n",
               params->BTree_root->obj_id, obj->obj_id);
        return NULL;
    }

    /* The object is not root and is not in tree. Insert it under its parent. */
    if((parent_page = __btree_find_parent(params->BTree_root, req->parent_addr)) == NULL) {
        ERROR("Parent of WiredTiger object %ld not found in WiredTiger tree. Cannot insert.\n",
               obj->obj_id);
        return NULL;
    } else {
        DEBUG_ASSERT(parent_page->obj_id == req->parent_addr);

        if (__btree_init_page(obj, params, req->page_type, parent_page, req->read_gen) != 0)
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
 * @param cache the cache
 * @return the object to be evicted
 */
static cache_obj_t *WT_to_evict(cache_t *cache, const request_t *req) {
  WT_params_t *params = (WT_params_t *)cache->eviction_params;

  INFO("WT_to_evict: \n");
  DEBUG_ASSERT(params->q_tail != NULL || cache->occupied_byte == 0);

  cache->to_evict_candidate_gen_vtime = cache->n_req;
  return params->q_tail;
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
    cache_obj_t *obj_to_evict = params->q_tail;
    DEBUG_ASSERT(params->q_tail != NULL);

    INFO("WT_evict: \n");

    params->q_tail = params->q_tail->queue.prev;
    if (likely(params->q_tail != NULL)) {
        params->q_tail->queue.next = NULL;
    } else {
        /* cache->n_obj has not been updated */
        DEBUG_ASSERT(cache->n_obj == 1);
        params->q_head = NULL;
    }

#if defined(TRACK_DEMOTION)
    if (cache->track_demotion)
        INFO("%ld demote %ld %ld\n", cache->n_req, obj_to_evict->create_time,
               obj_to_evict->misc.next_access_vtime);
#endif

    /* Remove object and its children from B-Tree */
    __btree_remove(cache, obj_to_evict);
    __btree_print(cache);
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
    if (obj == NULL) {
        return false;
    }
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    INFO("WT_remove: \n");

    remove_obj_from_list(&params->q_head, &params->q_tail, obj);
    /* XXX -- Remove from B-Tree */
    cache_remove_obj_base(cache, obj, true);
    return true;
}

static void WT_print_cache(const cache_t *cache) {
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    cache_obj_t *cur = params->q_head;
    /* print from the most recent to the least recent */
    if (cur == NULL) {
        printf("empty\n");
        return;
    }
    while (cur != NULL) {
        printf("%lu->", cur->obj_id);
        cur = cur->queue.next;
    }
    printf("END\n");
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

static int
__btree_init_page(cache_obj_t *obj, WT_params_t *cache_params, short page_type, cache_obj_t *parent_page, int read_gen) {
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
    cache_params->cache_inmem_bytes += obj->size;
    cache_params->btree_inmem_bytes += obj->size;
    return 0;
}

static inline void
__btree_print_node_nochildren(cache_obj_t *node) {
    int64_t parent_addr = 0;

    if (node->wt_page.parent_page != NULL)
        parent_addr = node->wt_page.parent_page->obj_id;
    printf("%lu %d [%lu]", node->obj_id, node->wt_page.page_type, parent_addr);
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
__btree_print(cache_t *cache){
    WT_params_t *params = (WT_params_t *)cache->eviction_params;

    printf("BEGIN ----------------------------------------\n");
    __btree_print_node(params->BTree_root, 0);
    printf("\nEND ----------------------------------------\n");
}

/*
 * Recursively remove the object's children from the B-Tree, and remove itself from its parent.
 */
static void
__btree_remove(cache_t *cache, cache_obj_t *obj) {
    int i;

    DEBUG_ASSERT(obj != NULL);

    if (obj->wt_page.page_type == WT_ROOT) {
        ERROR("WiredTiger: not allowed to remove root\n");
        return;
    }
    else if (obj->wt_page.page_type == WT_INTERNAL) {
        for (i = 0; i < getMapSize(obj->wt_page.children); i++)
            __btree_remove(cache, getValueAtIndex(obj->wt_page.children, i));
        deleteMap(obj->wt_page.children);
    }

    DEBUG_ASSERT(removePair(obj->wt_page.parent_page->wt_page.children, obj->obj_id));
    INFO("Removed object %lu from parent %lu (map %p)\n", obj->obj_id, obj->wt_page.parent_page->obj_id,
         obj->wt_page.parent_page->wt_page.children);

    cache_evict_base(cache, obj, true);
}
#ifdef __cplusplus
}
#endif
