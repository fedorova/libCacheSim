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

/* Maximum number of eviction queues */
#define WT_EVICT_QUEUE_MAX 3

typedef enum { /* Start position for eviction walk */
    WT_EVICT_WALK_NEXT,
    WT_EVICT_WALK_PREV,
    WT_EVICT_WALK_RAND_NEXT,
    WT_EVICT_WALK_RAND_PREV
} WT_EVICT_WALK_TYPE;

/* WiredTiger flags */
#define WT_READ_PREV 1
#define WT_RESTART   2
#define WT_NOTFOUND  3

#define WT_READGEN_OLDEST 1

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
static int __btree_evict_walk(cache_t *cache);
static cache_obj_t *__btree_evict_walk_trek(cache_obj_t *start);
static cache_obj_t *__btree_find_parent(cache_obj_t *start, obj_id_t obj_id);
static int __btree_init_page(cache_obj_t *obj, WT_params_t *params, short page_type, cache_obj_t *parent_obj, int read_gen);
static void __btree_print(cache_t *cache);
static int __btree_random_descent(cache_t *cache, cache_obj_t *evict_start);
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
    params->evict_slots = WT_EVICT_WALK_BASE + WT_EVICT_WALK_INCR;
    if ((params->evict_q = malloc(sizeof(cache_obj_t*) * params->evict_slots) == NULL))
        return NULL;

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

/*
 * WiredTiger's __evict_walk --
 *     Fill in the array by walking the next set of pages.
 *     In WiredTiger this function decides which tree should be walked.
 *     At the moment we are only supporting a single B-Tree, so we
 *     simply proceed with walking it.
 */

static int
__btree_evict_walk(const cache_t *cache)
{
    WT_params_t *params = (WT_params_t *)cache->eviction_params;

    int slot, max_entries;

    slot = params->evict_entries; /* first available evict entry */
    max_entries = WT_MIN(slot + WT_EVICT_WALK_INCR, params->evict_slots);
    __btree_evict_walk_tree(cache, max_entries, &slot);

}

static inline uint64_t
__btree_bytes_evictable(WT_params_t *param)
{
    return (params->btree_inmem_bytes - params->BTree_root->obj_size)
        * (100 + BTREE_OVERHEAD_PCT) / 100;
}

static inline uint64_t
__btree_cache_bytes_inuse(WT_params_t *param)
{
    return params->cache_inmem_bytes * (100 + BTREE_OVERHEAD_PCT) / 100;
}

/*
 * WiredTiger's __evict_walk_target --
 *    We currently only support the use case of read-only tree and thus
 *    only clean pages.
 */
static int
__btree_evict_walk_target(const cache_t *cache)
{
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    uint64_t btree_inuse, bytes_per_slot, cache_inuse;
    uint32_t target_pages;

    target_pages = 0;

/*
 * The minimum number of pages we should consider per tree.
 */
#define MIN_PAGES_PER_TREE 10

    /*
     * The target number of pages for this tree is proportional to the space it is taking up in
     * cache. Round to the nearest number of slots so we assign all of the slots to a tree
     * filling 99+% of the cache (and only have to walk it once).
     */
    btree_inuse = __btree_bytes_evictable(params);
    cache_inuse = __btree_cache_bytes_inuse(params);
    bytes_per_slot = 1 + cache_inuse / params->evict_slots;
    target_pages = (uint32_t)((btree_inuse + bytes_per_slot / 2) / bytes_per_slot);

    if (btree_inuse == 0)
        return (0);

    /*
     * There is some cost associated with walking a tree. If we're going to visit this tree, always
     * look for a minimum number of pages.
     */
    if (target_pages < MIN_PAGES_PER_TREE)
        target_pages = MIN_PAGES_PER_TREE;

    printf("__btree_evict_walk_target: returning %d target pages\n", target_pages);
    return (target_pages);
}

/*
 * WiredTiger's __tree_walk_internal --
 *     Move to the next/previous page in the tree.
 */
static int
 __btree_tree_walk_count(const cache_t *cache, cache_obj_t **nodep, int *walkcntp, int walk_flags)
{
    cache_obj_t node, *node_orig;
    Map children = NULL;
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    int slot;

    node_orig = *node;
    *node = NULL;

    if ((node = node_orig) == NULL) {
        node = params->BTree_root;
        children = node->wt_page.children;
        if (getMapSize(children) == 0)
            return 0;
        else if (walk_flags == WT_READ_PREV)
            slot = getMapSize(children) - 1;
        else
            slot = 0;
        goto descend;
    }

    if (node->wt_page.page_type == WT_ROOT)
        return 0;

    /*  if (getMapSize(children) == 0) { Mark internal pages with no children.
    /*             node->wt_page->read_gen = WT_READGEN_OLDEST; */

    for (;;) {
        if (getMapSize(node->wt_page.children) == 0 ||
            (flags == WT_READ_PREV && slot == 0) || (flags != WT_READ_PREV && slot == getMapSize(node->wt_page.children)-1) ) {
            /* Ascend to the parent to continue traversal? */
        }
        if (flags == WT_READ_PREV)
            slot--;
        else
            slot++;
        *walkcntp++;

        for (;;) {
descend:
            DEBUG_ASSERT(children != NULL); /* Make sure this is true if we ever get here from the loop */
            node = getValueAtIndex(children, slot);
            if (node->wt_page.page_type == WT_LEAF) {
                *nodep = node;
                DEBUG_ASSERT(node != node_orig);
                return 0;
            }
            /* We have an internal page */
            children = node->wt_page.children;
            /* The internal page is empty */
            if (getMapSize(children) == 0) {
                node->wt_page->read_gen = WT_READGEN_OLDEST;
                break; /* Here we need to ascend to the root to continue traversal */
            }
            if (walk_flags == WT_READ_PREV)
                slot = getMapSize(children) - 1;
            else
                slot = 0;
            continue; /* How do we make sure we don't overflow the slot? 
        }
    }
}

/*
 * WiredTiger's __evict_walk_tree --
 *     Get a few page eviction candidates from a single underlying file.
 */
static int
__btree_evict_walk_tree(const cache_t *cache, u_int max_entries, u_int *slotp)
{
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    cache_obj_t *evict, *end, *evict_queue, *last_parent, *ref, *start;
    bool give_up;
    int ret;
    uint32_t min_pages, target_pages, remaining_slots, walk_flags;
    uint64_t internal_pages_already_queued, internal_pages_queued, internal_pages_seen;
    uint64_t min_pages, pages_already_queued, pages_seen, pages_queued, refs_walked;

    evict_queue = params->evict_queue;
    give_up = false;
    last_parent = NULL;

    /*
     * Figure out how many slots to fill from this tree. Note that some care is taken in the
     * calculation to avoid overflow.
     */
    start = evict_queue + *slotp;
    remaining_slots = max_entries - *slotp;
    if (params->evict_walk_progress >= params->evict_walk_target) {
        params->evict_walk_target = __btree_evict_walk_target(cache);
        params->evict_walk_progress = 0;
    }
    target_pages = params->evict_walk_target - params->evict_walk_progress;

    if (target_pages > remaining_slots)
        target_pages = remaining_slots;

    if (target_pages == 0)
        return (0);

    min_pages = 10 * (uint64_t)target_pages;
    end = start + target_pages;

    /*
     * Choose a random point in the tree if looking for candidates in a tree with no starting point
     * set. This is mostly aimed at ensuring eviction fairly visits all pages in trees with a lot of
     * in-cache content.
     */
    switch (params->evict_start_type) {
    case WT_EVICT_WALK_NEXT:
        break;
    case WT_EVICT_WALK_PREV:
        walk_flags = WT_READ_PREV;
        break;
    case WT_EVICT_WALK_RAND_PREV:
        walk_flags = WT_READ_PREV;
    /* FALLTHROUGH */
    case WT_EVICT_WALK_RAND_NEXT:
        if (params->evict_ref == NULL)
            params->evict_ref = __btree_random_descent(cache);
        break;
    }
    /* XXX Do we need to assign evict_ref above since we are setting it to NULL here? */
    ref = params->evict_ref;
    params->evict_ref = NULL;

    internal_pages_already_queued = internal_pages_queued = internal_pages_seen = 0;
    for (evict = start, pages_already_queued = pages_queued = pages_seen = refs_walked = 0;
         evict < end && ret == 0;
         last_parent = ref == NULL ? NULL : ref->wt_page->parent_page,
             ret = __btree_tree_walk_count(cache, &ref, &refs_walked, walk_flags)) {
    }

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
__btree_init_page(cache_obj_t *obj, WT_params_t *cache_params, short page_type,
                  cache_obj_t *parent_page, int read_gen) {
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
 * WiredTiger's __wt_random_descent --
 *     Find a random page in a tree for eviction.
 *
 * In WiredTiger during random descent during eviction, we may get back a leaf page or an internal page.
 * An internal page would get returned either if it has no children or if we randomly selected a child that
 * is not present in the cache or has been deleted.  See __wt_page_in_func and __wt_random_descent.
 *
 * In our simulation, we would never return an internal page that has:
 * -- an uncached child, because uncached children are not part of our children map. Our code always selects
 * among cached children, so we will always return a cached child if one is present.
 * -- a deleted child. For the same reason as it wouldn't return the unached child and also for the reason that
 * we are not supporting read/write workloads for now – because we don’t emulate the code path evicting dirty pages.
 */
static cache_obj_t
__btree_random_descent(cache_t *cache)
{
    cache_obj_t *current_node;
    Map children;
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    int children_size;

    current_node = params->BTree_root;

    for (;;) {
        if (current_node->wt_page.page_type == WT_LEAF)
            break;
        children = current_node->wt_page.children;
        children_size = getMapSize(children);
        if (children_size != 0)
            current_node = getValueAtIndex(children, rand() % children_size);
        else break;
    }
    if (current != params->BTree_root)
        return current;
    return NULL;
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
