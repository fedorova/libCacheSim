/*
 * An emulation of the WiredTiger eviction algorithm for the
 * in-memory cache.
 */

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"
#include "../../dataStructure/map/map.h"
#include "WiredTiger.h"

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

static node_t *__btree_find_parent(node_t *start, obj_id_t obj_id);

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
    printf("cache_get_base: \n");
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
    node_t *parent_node = NULL;

    printf("WT_find: addr = %ld, parent_addr = %ld, read_gen = %d, type = %d\n",
           req->obj_id, req->parent_addr, req->read_gen, req->page_type);

    if (cache_obj != NULL && !cache_obj->wt_page.in_tree)
        printf("Warning: WiredTiger cache object not in tree\n");

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
    cache_obj_t *obj;
    node_t *new_node, *parent_node;
    WT_params_t *params = (WT_params_t *)cache->eviction_params;

    obj = cache_insert_base(cache, req);
    prepend_obj_to_head(&params->q_head, &params->q_tail, obj);

    printf("WT_insert: addr = %ld, parent_addr = %ld, read_gen = %d, type = %d\n",
           req->obj_id, req->parent_addr, req->read_gen, req->page_type);

    if (params->BTree_root.children == NULL) {
        /*
         * B-Tree is not initialized. If we are processing the first record in the
         * trace, we expect that record to identify the root node. If we see the
         * record that does not identify the root note and the BTree is not initialized,
         * this is an error.
         */
        if (req->parent_addr == 0) { /* Root is the only node with parent address zero. */
            if ((params->BTree_root.children = createMap()) == NULL) {
                printf("Could not allocate memory upon BTree initialization\n");
                return NULL;
            }
            params->BTree_root.cache_obj = obj;
            obj->wt_page.in_tree = true;
            obj->wt_page.page_type = WT_ROOT;
            return obj;
        } else {
            printf("Error: WiredTiger BTree has not been initialized\n");
                return NULL;
        }
    }

    if (obj->wt_page.in_tree) {
        printf("Error: new WiredTiger object already in tree\n");
        return NULL;
    }

    if (req->parent_addr == 0) {
        printf("Saw a new root record with root already present. Existing root address is %ld,"
               "this root access has address %ld. We do not support more than one tree.\n",
               params->BTree_root.cache_obj->obj_id, obj->obj_id);
        return NULL;
    }
    printf("Existing root address is %ld\n", params->BTree_root.cache_obj->obj_id);

    /* The object is not root and is not in tree. Insert it under its parent. */
    if((parent_node = __btree_find_parent(&params->BTree_root, req->parent_addr)) == NULL) {
        printf("Error: parent of object %ld not found in WiredTiger tree. Cannot insert.\n",
               obj->obj_id);
        return NULL;
    } else {
        DEBUG_ASSERT(parent_node->cache_obj->obj_id == req->parent_addr);

        if ((new_node = (node_t *)malloc(sizeof(node_t))) == NULL) {
            printf("Warning: could not allocate memory for a new WiredTiger node\n");
            goto err3;
        }
        if ((req->page_type == WT_INTERNAL) && (new_node->children = createMap()) == NULL) {
            printf("Warning: could not allocate memory for a new WiredTiger children map\n");
            goto err2;
        }
        if (insertPair(parent_node->children, obj->obj_id, (void*)new_node) != 0) {
            printf("Warning, WiredTiger: could not insert a child into the parent's map\n");
            goto err1;
        }
        obj->wt_page.in_tree = true;
        obj->wt_page.parent_addr = parent_node->cache_obj->obj_id;
        obj->wt_page.page_type = req->page_type;
        obj->wt_page.read_gen = req->read_gen;
        new_node->cache_obj = obj;
    }
    return obj;

  err1:
    deleteMap(new_node->children);
  err2:
    free(new_node);
  err3:
    return NULL;
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

    return;
#if 0
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    cache_obj_t *obj_to_evict = params->q_tail;
    DEBUG_ASSERT(params->q_tail != NULL);

    // we can simply call remove_obj_from_list here, but for the best performance,
    // we chose to do it manually
    // remove_obj_from_list(&params->q_head, &params->q_tail, obj)

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
        printf("%ld demote %ld %ld\n", cache->n_req, obj_to_evict->create_time,
               obj_to_evict->misc.next_access_vtime);
#endif

    cache_evict_base(cache, obj_to_evict, true);
#endif
}

/**
 * @brief remove the given object from the cache
 * note that eviction should not call this function, but rather call
 * `cache_evict_base` because we track extra metadata during eviction
 *
 * and this function is different from eviction
 * because it is used to for user trigger
 * remove, and eviction is used by the cache to make space for new objects
 *
 * it needs to call cache_remove_obj_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param obj
 */
static void WT_remove_obj(cache_t *cache, cache_obj_t *obj) {
  assert(obj != NULL);

  WT_params_t *params = (WT_params_t *)cache->eviction_params;

  remove_obj_from_list(&params->q_head, &params->q_tail, obj);
  cache_remove_obj_base(cache, obj, true);
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

  remove_obj_from_list(&params->q_head, &params->q_tail, obj);
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


static node_t *__btree_find_parent(node_t *start, obj_id_t parent_id) {

    Map children;
    node_t *found_node, *child_node;
    size_t i, map_size;

    if (parent_id == start->cache_obj->obj_id)
        return start;
    if ((children = start->children) == NULL)
        return NULL;
    if ((found_node = (node_t*)getValue(children, parent_id)) != NULL)
        return found_node;

    for (i = 0; i < getMapSize(children); i++) {
        child_node = getValueAtIndex(children, i);
        DEBUG_ASSERT(child_node != NULL);
        if ((found_node = __btree_find_parent(child_node, parent_id)) != NULL)
            return found_node;
    }
    return NULL;
}

#ifdef __cplusplus
}
#endif
