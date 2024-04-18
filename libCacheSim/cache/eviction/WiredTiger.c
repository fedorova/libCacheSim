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
#define WT_CACHE_EVICT_HARD 1
#define WT_EVICT_SCORE_BUMP 10
#define WT_EVICT_SCORE_MAX 100
#define WT_EVICT_WALK_BASE 300
#define WT_EVICT_WALK_INCR 100

#define WT_PAGE_EVICT_LRU 1

#define WT_READGEN_NOTSET 0
#define WT_READGEN_OLDEST 1
#define WT_READGEN_WONT_NEED 2
#define WT_READGEN_EVICT_SOON(readgen) \
    ((readgen) != WT_READGEN_NOTSET && (readgen) < WT_READGEN_START_VALUE)
#define WT_READGEN_START_VALUE 100
#define WT_READGEN_STEP 100

#define WT_RETRY_MAX 10
#define WT_THOUSAND 1000

typedef enum { /* Start position for eviction walk */
    WT_EVICT_WALK_NEXT,
    WT_EVICT_WALK_PREV,
    WT_EVICT_WALK_RAND_NEXT,
    WT_EVICT_WALK_RAND_PREV
} WT_EVICT_WALK_TYPE;

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

#define WT_MAX_MEMPAGE 32768 /* Default value for max leaf page size */

/* These flags must be powers of two -- so we have binary numbers with a single bit set. */
#define WT_CACHE_EVICT_CLEAN 1 << 0
#define WT_CACHE_EVICT_CLEAN_HARD 1 << 1

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
static cache_obj_t *__btree_find_parent(cache_obj_t *start, obj_id_t obj_id);
static int __btree_init_page(cache_obj_t *obj, WT_params_t *params, short page_type,
                             cache_obj_t *parent_obj, int read_gen);
static void __btree_node_index_slot(cache_obj_t *obj, Map *children, int *slotp);
static inline cache_obj_t * __btree_node_parent(cache_obj_t *node);
static char * __btree_page_to_string(cache_obj_t *obj);
static void __btree_print(const cache_t *cache);
static cache_obj_t * __btree_random_descent(const cache_t *cache);
static uint64_t __btree_readgen_new(const cache_t *cache);
static void __btree_remove(const cache_t *cache, cache_obj_t *obj);
static void __btree_tree_walk_count(const cache_t *cache, cache_obj_t **nodep, int *walkcntp,
                                   int walk_flags);

#define FLAG_SET(memory, value) (memory |= value)
#define FLAG_ISSET(memory, value) ((memory & value) != 0)
#define FLAG_CLEAR(memory, value) (((memory) &= ~(value)))

/* Eviction */
static int __evict_lru_pages(const cache_t *cache);
static int __evict_lru_walk(const cache_t *cache);
static int __evict_qsort_compare(const void *a, const void *b);
static inline bool ___evict_queue_empty(WT_evict_queue *queue);
static inline bool __evict_queue_full(WT_evict_queue *queue);
static inline void __evict_page_soon(cache_obj_t *obj);
static uint64_t __evict_priority(const cache_t *cache, cache_obj_t *score);
static bool __evict_queue_empty(WT_evict_queue *queue);
static bool __evict_queue_full(WT_evict_queue *queue);
static int __evict_walk(const cache_t *cache, WT_evict_queue *queue);
static int __evict_walk_target(const cache_t *cache);
static int __evict_walk_tree(const cache_t *cache, WT_evict_queue *queue, u_int max_entries,
                             u_int *slotp);
static bool __evict_update_work(const cache_t *cache);

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
    params->evict_slots = WT_EVICT_WALK_BASE + WT_EVICT_WALK_INCR;
    if ((params->evict_fill_queue.elements =
         malloc(sizeof(cache_obj_t*) * params->evict_slots)) == NULL)
        return NULL;
    memset(params->evict_fill_queue.elements, 0, sizeof(cache_obj_t*) * params->evict_slots);
    params->eviction_trigger = 95; /* default eviction trigger in WiredTiger */
    params->cache_size = ccache_params.cache_size;
    params->splitmempage = 8 * WT_MAX_MEMPAGE / 10;
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

    INFO("WT_find: addr = %ld, parent_addr = %ld, read_gen = %d, type = %d, op = %d\n",
         req->obj_id, req->parent_addr, req->read_gen, req->page_type, req->operation_type);

    if (cache_obj != NULL) {
        if (!cache_obj->wt_page.in_tree || cache_obj->wt_page.page_type != req->page_type
            || cache_obj->wt_page.parent_page->obj_id != req->parent_addr) {
            ERROR("Cached WiredTiger object changed essential properties.\n");
        }
        else if (cache_obj->wt_page.read_gen != req->read_gen) {
            cache_obj->wt_page.read_gen = req->read_gen;
            INFO("Updating read generation to %d\n", req->read_gen);
        }
    }

    /*
     * For all operation types except WT_ACCESS, the simulation code above is (in sim.c)
     * will refrain from updating access count and miss count, in effect "ignoring"
     * these operations. We use these special operation types to modify our cache state
     * without running the normal simulation.
     */
#ifdef STRICT
    INFO("Entering STRICT\n");
    /* STRICT mode: we mimic WiredTiger actions, don't do any simulation of our own */
    if (req->operation_type == WT_EVICT) {
        if (cache_obj == NULL)
            ERROR("WiredTiger evicts object that we do not have\n");
        INFO("PREEMPTIVE EVICTION\n");
        __btree_remove(cache, cache_obj);
        /*
         * Below we will be returning an invalid pointer. We do that,
         * because we don't want the cache code above us to register a miss and
         * insert back the object that we just evicted. The code above us does
         * not dereference the pointer, so it's safe to return garbage.
         */
        cache_obj = (cache_obj_t *) 0xDEADBEEF;
    }
#endif
#define STRICT_1
#ifdef STRICT_1
    cache_obj_t *evict_victim;
    WT_evict_queue *queue =  &params->evict_fill_queue;

    INFO("Entering STRICT_1\n");
    if (req->operation_type != WT_ACCESS) {
        if (cache_obj == NULL)
            WARN("WiredTiger makes an evict operation %d on an object that we do not have\n",
                  req->operation_type);
    }
    if (req->operation_type == WT_EVICT_ADD) {
        if (queue->evict_entries == params->evict_slots) {
            /*
             * We have reached the tail of the queue.
             */
            WARN("EVICT QUEUE full in STRICT_1 mode, but WT adds %s\n",
                 __btree_page_to_string(cache_obj));
        } else
                queue->elements[queue->evict_entries++] = cache_obj;

        /* Sort the queue */
        qsort(queue->elements, queue->evict_entries, sizeof(cache_obj_t*),
              __evict_qsort_compare);

        /* Trip any null entries */
        while (queue->evict_entries >= 0 && queue->elements[queue->evict_entries--] == NULL);

        /* Reset the pointer so that eviction begins to evict from the head of the queue */
        if (queue->evict_entries < 0) {
            queue->evict_entries = 0;
            queue->evict_current = -1;
            WARN("Evict queue empty in STRICT_1\n");
        } else
            queue->evict_current = 0;
    }
    else if (req->operation_type == WT_EVICT) {

        /* Evict the top candidate */
        if (queue->evict_current == params->evict_slots)
            WARN("Evicting beyond the tail of EVICT QUEUE in STRICT_1 mode\n");
        if (queue->evict_current == -1)
            evict_victim == NULL;
        else {
            evict_victim = queue->elements[queue->evict_current];
            queue->elements[queue->evict_current++] = NULL;
            __btree_remove(cache, evict_victim);
        }
        if (evict_victim == NULL)
            WARN("WT evicts %s, but our queue is empty\n", __btree_page_to_string(cache_obj));
        else {
            /* Compare our candidate with the WiredTiger candidate */
            printf("Simulation evicted: %s\n", __btree_page_to_string(evict_victim));
            printf("WiredTiger evicted: %s\n", __btree_page_to_string(cache_obj));
        }
    }
    if (req->operation_type != WT_ACCESS)
        cache_obj = (cache_obj_t *) 0xDEADBEEF;
#endif

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
            if (__btree_init_page(obj, params, WT_ROOT, NULL /* parent */, 0 /* read gen */) != 0)
                return NULL;
            return obj;
        } else {
            ERROR("WiredTiger BTree has not been initialized\n");
                return NULL;
        }
    }

    if (obj->wt_page.in_tree) {
        printf("Error: new WiredTiger object already in tree\n");
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

    ERROR("WT_evict: \n");

     /*
      * Increment the shared read generation. Do this occasionally even if eviction is not
      * currently required, so that pages have some relative read generation when the eviction
      * server does need to do some work.
      */
    params->read_gen++;

    __evict_update_work(cache);
    /* Under what conditions does the eviction server keep evicting? XXX */
    __evict_lru_walk(cache);

    /* Keep evicting pages until there's something left in queues. */
    while (__evict_lru_pages(cache) == 0)
        __btree_print(cache);

    INFO("Evict queue empty\n");

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
    return params->pages_inmem - params->pages_evicted;
}

/*
 * WiredTiger's __evict_lru_walk --
 *     Add pages to the LRU queue to be evicted from cache.
 *
 *     1. Find the queue we will fill. In the current implementation we assume a single
 *     queue only. (This is because we are single threaded, and queue filling and eviction
 *     is done by the same thread.) If the queue is full, no need to fill it, we are done.
 *
 *     2. Call __evict_walk, which will fill the queue.
 *
 *     3. Decide how many of the filled entries are going to be candidates for
 *     eviction. That is determined by how aggressively we want to evict and on the
 *     read generations of the filled entries.
 */
static int
__evict_lru_walk(const cache_t *cache)
{
    WT_evict_queue *queue;
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    int ret;
    u_int candidates, entries;
    uint64_t read_gen_oldest;

    INFO("__evict_lru_walk");

    if (params->evict_empty_score > 0)
        params->evict_empty_score--;

    /*
     * We will fill the evict fill queue.
     * TODO: Assume we have a single queue for now.
     */
    queue = &params->evict_fill_queue;

    /* If the queue is full, we are done. */
    if (__evict_queue_full(queue)) {
        WARN("evict_lru_walk: queue full, bailing out\n");
        return 0;
    }

    /*
     * If the queue we are filling is empty, pages are being requested faster than they are being
     * queued.
     */
    if (__evict_queue_empty(queue)) {
        if (FLAG_ISSET(params->evict_flags, WT_CACHE_EVICT_HARD))
            params->evict_empty_score =
                MIN(params->evict_empty_score + WT_EVICT_SCORE_BUMP, WT_EVICT_SCORE_MAX);
        params->cache_eviction_queue_empty++;
        INFO("__evict_lru_walk: queue empty\n");
    } else
        params->cache_eviction_queue_not_empty++;

    /*
     * Get some more pages to consider for eviction.
     *
     * If the walk is interrupted, we still need to sort the queue: the next walk assumes there are
     * no entries beyond WT_EVICT_WALK_BASE.
     */
    ret = __evict_walk(cache, queue);
    (void) ret; /* No need to check for now. */

    /*
     * Sort the evict queue and set the number of non-empty elements
     */
    INFO("__evict_lru_walk: sorting the queue\n");

    qsort(queue->elements, queue->evict_entries, sizeof(cache_obj_t*), __evict_qsort_compare);
    entries = queue->evict_entries;

    /*
     * If we have more entries than the maximum tracked between walks, clear them. Do this before
     * figuring out how many of the entries are candidates so we never end up with more candidates
     * than entries.
     */
    INFO("__evict_lru_walk: clearing unneeded entries from a queue with %u entries\n", entries);
    while (entries > WT_EVICT_WALK_BASE) {
        printf("entries = %u\n", entries);
        queue->elements[--entries] = NULL;
    }

    printf("done\n");
    printf("entries = %u\n", entries);
    /* Re-adjust the entries to the first non-empty slot */
    while (entries > 0 && queue->elements[entries-1] == NULL) {
        entries--;
        printf("entries = %d\n", entries);
    }
    queue->evict_entries = entries;
    INFO("__evict_lru_walk: ended up with %d entries after clearing\n", entries);

    if (entries == 0) {
        /*
         * If there are no entries, there cannot be any candidates. Make sure we don't read
         * past the end of the candidate list.
         */
        queue->evict_candidates = 0;
        queue->evict_current = -1;
        INFO("__evict_lru_walk: zero entries. Set evict_current to -1\n");
        return (0);
    }
    if (params->evict_aggressive)
        queue->evict_candidates = entries;
    else {
       /*
         * Find the oldest read generation apart that we have in the queue, used to set the initial
         * value for pages read into the system. The queue is sorted, find the first "normal"
         * generation.
         */
        read_gen_oldest = WT_READGEN_START_VALUE;
        for (candidates = 0; candidates < entries; ++candidates) {
            read_gen_oldest = queue->elements[candidates]->wt_page.evict_score;
            if (!WT_READGEN_EVICT_SOON(read_gen_oldest))
                break;
        }
        INFO("__evict_lru_walk: gathered %d candidates out of %d entries."
             "Oldest generation observed: %lu\n", candidates, entries, read_gen_oldest);

        /*
         * Take all candidates if we only gathered pages with an oldest
         * read generation set.
         *
         * We normally never take more than 50% of the entries but if
         * 50% of the entries were at the oldest read generation, take
         * all of them.
         */
        if (WT_READGEN_EVICT_SOON(read_gen_oldest))
            queue->evict_candidates = entries;
        else if (candidates > entries / 2)
            queue->evict_candidates = candidates;
        else {
            /*
             * Take all of the urgent pages plus a third of ordinary candidates (which could be
             * expressed as WT_EVICT_WALK_INCR / WT_EVICT_WALK_BASE). In the steady state, we want
             * to get as many candidates as the eviction walk adds to the queue.
             *
             * That said, if there is only one entry, which is normal when populating an empty file,
             * don't exclude it.
             */
            queue->evict_candidates = 1 + candidates + ((entries - candidates) - 1) / 3;
            params->read_gen_oldest = read_gen_oldest;
        }
        INFO("__evict_lru_walk: ended up with %d evict_candidates. Set read_gen_oldest to %lu\n",
             queue->evict_candidates, read_gen_oldest);
    }
    params->cache_eviction_pages_queued_post_lru += queue->evict_candidates;
    queue->evict_current = 0;
    return (ret);
}

/*
 * WiredTiger's __evict_walk --
 *     Fill in the array by walking the next set of pages.
 *     In WiredTiger this function decides which tree should be walked.
 *     At the moment we are only supporting a single B-Tree, so we
 *     simply proceed with walking it.
 */

static int
__evict_walk(const cache_t *cache, WT_evict_queue *queue)
{
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    u_int max_entries, retries, slot, start_slot, total_candidates;
    int ret;

    start_slot = slot = queue->evict_entries; /* first available evict entry */
    max_entries = MIN(slot + WT_EVICT_WALK_INCR, params->evict_slots);

    /* This executes if  WT_CACHE_EVICT_CLEAN is set. TODO: update for writes. */
    total_candidates = __cache_pages_inuse(params);
    max_entries = MIN(max_entries, 1 + total_candidates / 2);

    printf("total_candidates = %d, __cache_pages_inuse = %lu\n",
           total_candidates, __cache_pages_inuse(params));

    INFO("__evict walk: starting at slot %d with %d max entries\n", slot, max_entries);

  retry:
    while (slot < max_entries) {
        INFO("evict_walk: calling __evict_walk_tree in a loop\n");
        __evict_walk_tree(cache, queue, max_entries, &slot);
    }

    /*
     * Repeat the walks a few times if we don't find enough pages. Give up when we have some
     * candidates and we aren't finding more.
     */
    if (slot < max_entries &&
        (retries < 2 ||
         (retries < WT_RETRY_MAX && (slot == queue->evict_entries || slot > start_slot)))) {
        start_slot = slot;
        ++retries;
        INFO("__evict walk: will retry walk with %d slot, %d retries\n", slot, retries);
        goto retry;
    }

    /*
     * If we didn't find any entries on a walk when we weren't interrupted, let our caller know.
     */
    if (queue->evict_entries == slot)
        ret = -1;

    INFO("__evict walk: old evict_entries = %d, new evict_entries = %d\n",
         queue->evict_entries, slot);
    queue->evict_entries = slot;
    return (ret);
}

/*
 * WiredTiger's __evict_walk_target --
 *    We currently only support the use case of read-only tree and thus
 *    only clean pages.
 */
static int
__evict_walk_target(const cache_t *cache)
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

    if (btree_inuse == 0) {
        INFO("Btree not in use. Setting evict target to zero.\n");
        return (0);
    }

    /*
     * There is some cost associated with walking a tree. If we're going to visit this tree, always
     * look for a minimum number of pages.
     */
    if (target_pages < MIN_PAGES_PER_TREE)
        target_pages = MIN_PAGES_PER_TREE;

    INFO("__btree_evict_walk_target: returning %d target pages\n", target_pages);
    return (target_pages);
}

/*
 * WiredTiger's __tree_walk_internal --
 *     Move to the next/previous page in the tree.
 */
static void
 __btree_tree_walk_count(const cache_t *cache, cache_obj_t **nodep, int *walkcntp, int walk_flags)
{
    cache_obj_t *node, *node_orig;
    Map children = NULL;
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    int slot, prev;

    node_orig = *nodep;
    *nodep = NULL;
    slot = 0;

    prev = (walk_flags == WT_READ_PREV)?1:0;

    if (node_orig == NULL)
        INFO("Original node is NULL\n");
    else
        INFO("Original node is %s\n", __btree_page_to_string(node_orig));

    if ((node = node_orig) == NULL) {
        INFO("Starting walk from root\n");
        node = params->BTree_root;
        children = node->wt_page.children;
        if (getMapSize(children) == 0)
            return;
        else if (prev)
            slot = getMapSize(children) - 1;
        else
            slot = 0;
        goto descend;
    } else if (node->wt_page.page_type == WT_ROOT)
        return;

    /* Figure out the current slot in the WT_REF array. */
    __btree_node_index_slot(node, &children, &slot);

    INFO("Index slot is %d for page %s in map of size %ld \n",
         slot, __btree_page_to_string(node), getMapSize(children));

    /* If we are at the edge of the page, ascend to parent */
    while ((prev && slot == 0) ||
           (!prev && slot == getMapSize(__btree_node_parent(node)->wt_page.children)- 1)) {

        /* Get the parent */
        node = __btree_node_parent(node);

        /* We never evict the root page for now */
        if (node->wt_page.page_type == WT_ROOT)
            return;

        /* Find the children map containing the new node and the node's slot in that map */
        __btree_node_index_slot(node, &children, &slot);

        if ((node->wt_page.page_type == WT_INTERNAL) && (getMapSize(node->wt_page.children) == 0))
            __evict_page_soon(node);

        *nodep = node;
        DEBUG_ASSERT(node != node_orig);
        return;
    }

    /* Otherwise increment or decrement the slot */
    if (prev) --slot;
    else ++slot;

    if (walkcntp != NULL)
        ++*walkcntp;
    INFO("Walking the tree, iteration %d\n",  *walkcntp);

    for (;;) {
      descend:
        DEBUG_ASSERT(children != NULL);
        INFO("Using slot %d in parent %s\n", slot, __btree_page_to_string(node));

        node = getValueAtIndex(children, slot);

        if (node->wt_page.page_type == WT_LEAF) {
            *nodep = node;
            DEBUG_ASSERT(node != node_orig);
            INFO("Returning leaf page %s\n", __btree_page_to_string(node));
            return;
        }

        /* We have an internal page */
        children = node->wt_page.children;

        /* The internal page is empty */
        if (getMapSize(children) == 0) {
            /* This is an empty internal page. Prioritize for eviction and return. */
            __evict_page_soon(node);
            *nodep = node;
            INFO("Returning empty internal page %s\n", __btree_page_to_string(node));
            return;
        }
        if (prev)
            slot = getMapSize(children) - 1;
        else
            slot = 0;
        continue;
    }
}

/*
 * WiredTiger's __evict_walk_tree --
 *     Get a few page eviction candidates from a single underlying file.
 */
static int
__evict_walk_tree(const cache_t *cache, WT_evict_queue *queue, u_int max_entries, u_int *slotp)
{
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    cache_obj_t *evict, *end, *last_parent, *ref, *start;
    bool give_up;
    int ret, refs_walked, restarts;
    uint32_t target_pages, remaining_slots, walk_flags;
    uint64_t internal_pages_already_queued, internal_pages_queued, internal_pages_seen;
    uint64_t min_pages, pages_already_queued, pages_seen, pages_queued;

    give_up = false;
    last_parent = NULL;
    ref = NULL;
    restarts = 0;
    ret = 0;

    INFO("__evict_walk_tree: to begin filling queue at slot %d\n", *slotp);
    /*
     * Figure out how many slots to fill from this tree. Note that some care is taken in the
     * calculation to avoid overflow.
     */
    start = queue->elements[*slotp];
    remaining_slots = max_entries - *slotp;
    if (params->evict_walk_progress >= params->evict_walk_target) {
        params->evict_walk_target = __evict_walk_target(cache);
        params->evict_walk_progress = 0;
    }
    INFO("__evict_walk_tree: target is %d, progress is %d, target pages is %d\n",
         params->evict_walk_target, params->evict_walk_progress, target_pages);
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

    /* We save the last point where we walked the tree. */
    ref = params->evict_ref;
    params->evict_ref = NULL;

    if (ref == NULL)
        INFO("__evict_walk_tree: starting with NULL reference\n");
    else
        INFO("__evict_walk_tree: starting with node %s\n",
             __btree_page_to_string(ref));

    internal_pages_already_queued = internal_pages_queued = internal_pages_seen = 0;
    for (evict = start, pages_already_queued = pages_queued = pages_seen = refs_walked = 0;
         evict < end && ret == 0;
         last_parent = (ref == NULL ? NULL : ref->wt_page.parent_page),
             __btree_tree_walk_count(cache, &ref, &refs_walked, walk_flags)) {
        /*
         * Below are a bunch of conditions deciding whether we should queue this page for
         * eviction and whether we should keep walking the tree.
         *
         * The first condition, in addition to the logic below, also checks if we have
         * __wt_cache_aggressive set and if the current B-Tree is a history store.
         * TODO: check if we need to add those conditions.
         */
        give_up = pages_seen > min_pages &&
          (pages_queued == 0 || (pages_seen / pages_queued) > (min_pages / target_pages));

        INFO("evict_walk_tree: after __btree_tree_walk_count: give_up = %d, ref = %p\n",
             give_up, ref);
        if (give_up) {
            switch (params->evict_start_type) {
            case WT_EVICT_WALK_NEXT:
                params->evict_start_type = WT_EVICT_WALK_PREV;
                break;
            case WT_EVICT_WALK_PREV:
                params->evict_start_type = WT_EVICT_WALK_RAND_PREV;
                break;
            case WT_EVICT_WALK_RAND_PREV:
                params->evict_start_type = WT_EVICT_WALK_RAND_NEXT;
                break;
            case WT_EVICT_WALK_RAND_NEXT:
                params->evict_start_type = WT_EVICT_WALK_NEXT;
                break;
            }
            /*
             * We differentiate the reasons we gave up on this walk and increment the stats
             * accordingly.
             */
            if (pages_queued == 0)
                params->cache_eviction_walks_gave_up_no_targets++;
            else
                params->cache_eviction_walks_gave_up_ratio++;
            break;
        }
        if (ref == NULL) {
            params->cache_eviction_walks_ended++;

            if (++restarts == 2) {
                params->cache_eviction_walks_stopped++;
                break;
            }
            params->cache_eviction_walks_started++;
            continue;
        }

        ++pages_seen;

        if (ref->wt_page.page_type == WT_ROOT)
            continue;

        /*
         * TODO: here the WiredTiger code checks if the page is modified. We are not
         * doing this for now. Will need to change in the future.
         */
        ref->wt_page.evict_pass_gen = params->evict_pass_gen;

        if (ref->wt_page.page_type == WT_INTERNAL)
             internal_pages_seen++;

        if (ref->wt_page.evict_flags == WT_PAGE_EVICT_LRU) {
            pages_already_queued++;
            if (ref->wt_page.page_type == WT_INTERNAL)
                internal_pages_already_queued++;
            continue;
        }

         /*
          * Here WiredTiger does the following check:
          * Don't queue dirty pages in trees during checkpoints.
          * TODO: We skip this check for now.
          */
        if (ref->wt_page.read_gen == WT_READGEN_NOTSET)
            ref->wt_page.read_gen = __btree_readgen_new(cache);

        /*
         * Then in the original code we have the logic for pages being forcibly evicted,
         * but it is only for pages marked as modified. Since we are not addressing
         * workloads with modified pages for now, we skip this logic.
         */

        /*
         * Next follows the logic prioritizing eviction of history store pages over other
         * pages if history store dirty content is dominating the cache. We skip this for now.
         * TODO: Enable later.
         */

        /*
         * Next, WiredTiger does the following check:
         * Pages that are empty or from dead trees are fast-tracked.
         *
         * TODO: Add dead tree checking when we get there.
         *
         * TODO: Figure out if an internal page is empty if it has no cached children,
         * or if it must have no children at all to be considered empty.
         * For now we can't have empty leaf pages, because we do not support workloads
         * that delete data.
         */

        /*
         * Next, WiredTiger does the check if this page is a metadata page not yet visible
         * to all transactions. TODO: skip this for now.
         */

        /*
         * Next, WiredTiger checks if we want this page based on flags set and the page
         * properties. The flags have to do with pages being dirty. That is, the flags
         * might instruct us to skip pages that are modified. We are not supporting
         * workloads that dirty pages for now, so skip this check.
         * TODO: add later.
         */

        /*
         * Don't evict internal pages with children in cache
         *
         * Also skip internal page unless we get aggressive, the tree is idle (indicated by the
         * tree being skipped for walks), or we are in eviction debug mode.
         *
         * TODO: We don't need eviction debug mode and we currently only support a single tree.
         */
        if (ref->wt_page.page_type == WT_INTERNAL) {
            if (getMapSize(ref->wt_page.children) > 0)
                continue;
            if (!params->evict_aggressive)
                continue;
        }

        /*
         * Next, the WiredTiger code checks the transaction state of the page.
         * TODO: Add that later.
         */
        INFO("About to add to queue at slot %d with max entries %d\n", *slotp, max_entries);
        DEBUG_ASSERT(params->evict_ref == NULL);
        DEBUG_ASSERT(*slotp < max_entries);
        printf("Adding page %s to queue at slot %d\n", __btree_page_to_string(ref), (*slotp));
        queue->elements[(*slotp)++] = ref;
        ref->wt_page.evict_score = __evict_priority(cache, ref);
        ref->wt_page.evict_flags = WT_PAGE_EVICT_LRU;
        ++evict;
        ++pages_queued;
        ++params->evict_walk_progress;

        INFO("__evict_walk_tree: slot=%d, pages_queued = %ld\n", *slotp, pages_queued);

        /* Count internal pages queued. */
        if (ref->wt_page.page_type == WT_INTERNAL)
            internal_pages_queued++;
    }

    params->cache_eviction_pages_queued += ((u_int)(evict - start));

    /*
     * WiredTiger code: "If we couldn't find the number of pages we were looking for,
     * skip the tree next time."
     *
     * At the time of the writing the above is not relevant to us, because we are only supporting
     * a single tree, so if we have to evict we must walk that tree.
     *
     * TODO -- not sure if I need to track the evict walk period given that we are supporting
     * a single tree. Keep it for now.
     */
    if (pages_queued < target_pages / 2)
        params->evict_walk_period = MIN(MAX(1, 2 * params->evict_walk_period), 100);
    else if (pages_queued == target_pages) {
        params->evict_walk_period = 0;
        /*
         * WiredTiger code does this:
         * If there's a chance the Btree was fully evicted, update the evicted flag in the handle.
         * This isn't useful to us at the time of the writing, since we are only supporting a single
         * B-Tree for now.
         */
    } else if (params->evict_walk_period > 0)
        params->evict_walk_period /= 2;

    /*
     * Give up the walk occasionally.
     *
     * If we land on a page requiring forced eviction, or that isn't an ordinary in-memory page,
     * move until we find an ordinary page: we should not prevent exclusive access to the page until
     * the next walk. XXX -- does this apply to us?
     */
    if (ref != NULL) {
        if (ref->wt_page.page_type == WT_ROOT || evict == start || give_up) {
            if (restarts == 0)
                params->cache_eviction_walks_abandoned++;
            ref = NULL;
        } else
            while (ref != NULL && ref->wt_page.read_gen == WT_READGEN_OLDEST)
                __btree_tree_walk_count(cache, &ref, &refs_walked, walk_flags);
        params->evict_ref = ref;
    }

    params->cache_eviction_walk += refs_walked;
    params->cache_eviction_pages_seen += pages_seen;
    params->cache_eviction_pages_already_queued += pages_already_queued;
    params->cache_eviction_internal_pages_seen += internal_pages_seen;
    params->cache_eviction_internal_pages_already_queued +=  internal_pages_already_queued;
    params->cache_eviction_internal_pages_queued += internal_pages_queued;
    params->cache_eviction_walk_passes++;
    return (0);
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
    cache_params->cache_inmem_bytes += obj->obj_size;
    cache_params->btree_inmem_bytes += obj->obj_size;
    cache_params->pages_inmem++;
    if (read_gen != WT_READGEN_NOTSET) {
        if (cache_params->read_gen_oldest == WT_READGEN_NOTSET ||
            read_gen < cache_params->read_gen_oldest)
            cache_params->read_gen_oldest = read_gen;
    }
    if (read_gen > cache_params->read_gen)
        cache_params->read_gen = read_gen;

    cache_params->btree_total_pages++;
    if (page_type != WT_LEAF)
        cache_params->btree_internal_pages++;
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
#if 0
    printf("BEGIN ----------------------------------------\n");
    __btree_print_node(params->BTree_root, 0);
    printf("\nEND ----------------------------------------\n");
#endif
}

/*
 * WiredTiger's __wt_random_descent --
 *     Find a random page in a tree for eviction.
 *
 * In WiredTiger during random descent during eviction, we may get back a leaf page or
 * an internal page. An internal page would get returned either if it has no children or
 * if we randomly selected a child that is not present in the cache or has been deleted.
 * See __wt_page_in_func and __wt_random_descent.
 *
 * In our simulation, we would never return an internal page that has:
 * -- an uncached child, because uncached children are not part of our children map.
 * Our code always selects among cached children, so we will always return a cached child if
 * one is present.
 * -- a deleted child. For the same reason as it wouldn't return the unached child and also
 * for the reason that we are not supporting read/write workloads for now – because we don’t
 * emulate the code path evicting dirty pages.
 * XXX -- double check the implementation. Do we need a starting node?
 */
static cache_obj_t *
__btree_random_descent(const cache_t *cache)
{
    cache_obj_t *current_node;
    Map children;
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    int children_size;

    current_node = params->BTree_root;

    INFO("__btree_random_descent");

    for (;;) {
        if (current_node->wt_page.page_type == WT_LEAF)
            break;
        children = current_node->wt_page.children;
        children_size = getMapSize(children);
        if (children_size != 0)
            current_node = getValueAtIndex(children, rand() % children_size);
        else break;
    }
    if (current_node != params->BTree_root)
        return current_node;
    return NULL;
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
        return;
    }
    else if (obj->wt_page.page_type == WT_INTERNAL) {
        if (getMapSize(obj->wt_page.children) > 0)
            WARN("Removing internal page with children: %s\n", __btree_page_to_string(obj));
        for (i = 0; i < getMapSize(obj->wt_page.children); i++)
            __btree_remove(cache, getValueAtIndex(obj->wt_page.children, i));
        deleteMap(obj->wt_page.children);
    }

    DEBUG_ASSERT(removePair(obj->wt_page.parent_page->wt_page.children, obj->obj_id));
    INFO("Removed object %lu from parent %lu (map %p)\n", obj->obj_id,
         obj->wt_page.parent_page->obj_id,
         obj->wt_page.parent_page->wt_page.children);
    params->pages_evicted++;
    params->btree_total_pages--;
    params->pages_inmem--;
    params->cache_inmem_bytes -= obj->obj_size;
    params->btree_inmem_bytes -= obj->obj_size;

    if (obj->wt_page.page_type == WT_INTERNAL)
        params->btree_internal_pages--;

    cache_evict_base((cache_t *)cache, obj, true);
}

/*
 * We use this global buffer for pretty-printing the page. We are single-threaded, so it's okay
 * to use this global buffer. We do this, so that we don't have to dynamically allocate the memory
 * and the caller doesn't need to free it for us.
 */
#define PAGE_PRINT_BUFFER_SIZE 50
char page_buffer[PAGE_PRINT_BUFFER_SIZE];

static char *
__btree_page_to_string(cache_obj_t *obj) {

    snprintf((char*)&page_buffer, PAGE_PRINT_BUFFER_SIZE,
             "page %lu [%lu], %s, read-gen: %ld", obj->obj_id,
             (obj->wt_page.parent_page == NULL)?0:obj->wt_page.parent_page->obj_id,
             (obj->wt_page.page_type == WT_LEAF)?"leaf":(obj->wt_page.page_type == WT_INTERNAL)?"int":"root",
             obj->wt_page.read_gen);
    return (char *) page_buffer;
}


/*
 * A comparison function for the sorting algorithm.
 */
static int
__evict_qsort_compare(const void *a_arg, const void *b_arg) {
    const cache_obj_t *a, *b;
    uint64_t a_score, b_score;

    a = *(cache_obj_t **)a_arg;
    b = *(cache_obj_t **)b_arg;
    a_score = (a == NULL ? UINT64_MAX : a->wt_page.evict_score);
    b_score = (b == NULL ? UINT64_MAX : b->wt_page.evict_score);

    return ((a_score < b_score) ? -1 : (a_score == b_score) ? 0 : 1);
}

static uint64_t
__evict_priority(const cache_t *cache, cache_obj_t *ref) {
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    uint64_t read_gen;

    /* Any page set to the oldest generation should be discarded. */
    if (WT_READGEN_EVICT_SOON(ref->wt_page.read_gen))
        return (WT_READGEN_OLDEST);

    /* Any page from a dead tree is a great choice. */
    /* TODO: We are not tracking dead B-Trees yet. */

    /* Any empty page (leaf or internal), is a good choice. */
    /* TODO: We are not supporting deletions yet, so this is for the future. */

    /* Any large page in memory is likewise a good choice. */
    if (ref->obj_size > params->splitmempage)
        return (WT_READGEN_OLDEST);

    /*
     * TODO: The following if-statement performs a different adjustment for modified
     * pages, which we for now skip.
     */
    read_gen = ref->wt_page.read_gen;

    read_gen += params->evict_priority; /* Think this is only set for bloom and metadata cursors. */

#define WT_EVICT_INTL_SKEW WT_THOUSAND
    if (ref->wt_page.page_type == WT_INTERNAL)
        read_gen += WT_EVICT_INTL_SKEW;

    return read_gen;
}

/*
 * Evict a page from the LRU queue. In WiredTiger we may find pages that are unavailable.
 * Here we are single threaded and not supporting updates for now, so this shouldn't
 * happen. So we are just executing the logic of __evict_page here.
 */
static int
__evict_lru_pages(const cache_t *cache) {
    cache_obj_t *evict_victim;
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    WT_evict_queue *queue;

    INFO("__evict_lru_pages \n");

    /* We assume for now that there is only one queue */
    queue = &params->evict_fill_queue;

    if (__evict_queue_empty(queue)) {
        WARN("__evict_lru_pages(): returning QUEUE EMPTY\n");
        return -1;
    }

    printf("Here. Evict_current is %d\n", queue->evict_current);
    /*
     * Get the page at the queue's evict_current position.
    /* Update the current evict position for the next eviction to start at the right place.
    */
    DEBUG_ASSERT(queue->evict_current <= params->evict_slots);
    evict_victim = queue->elements[queue->evict_current];

    /* XXX check why this may happen */
    if (evict_victim == NULL) {
        WARN("Evict victim is NULL\n");
        return -1;
    }
    /*
     * Decide if we want to split the page (TODO).
    /* Don't support modified pages for now.
    */

    /* Make sure the page has no children */
    if (evict_victim->wt_page.page_type == WT_INTERNAL && getMapSize(evict_victim->wt_page.children) != 0)
        ERROR("WiredTiger: cannot evict internal page with children.\n");

    /* Remove the page from evict queue and from the tree */
    queue->elements[queue->evict_current] = NULL;
    __btree_remove(cache, evict_victim);

    if (queue->evict_current + 1 < queue->evict_candidates)
        queue->evict_current++;
    else
        queue->evict_current = -1;

    return 0;
}


static bool
__evict_queue_empty(WT_evict_queue *queue) {

    if (queue->evict_current == -1)
        return true;
    if(queue->evict_current >= queue->evict_candidates)
        return true;
    return false;
}

static inline bool
__evict_queue_full(WT_evict_queue *queue) {
    if (queue->evict_candidates > 500) {
        printf("CRAZY number of candidates!\n");
        _exit(0);
    }
    return (queue->evict_current == 0 && queue->evict_candidates != 0);
}

static inline bool
__eviction_clean_needed(const cache_t *cache) {
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    uint64_t bytes_inuse, bytes_max;

    return (bytes_inuse > (params->eviction_trigger * bytes_max) / 100);
}

/*
 * WiredTiger eviction sets a bunch of flags for dirty pages.
 * We don't emulate updates, so for now we only deal with the flags
 * relevant for clean pages.
 */
static bool
__evict_update_work(const cache_t *cache) {
    WT_params_t *params = (WT_params_t *)cache->eviction_params;
    uint64_t bytes_inuse, bytes_max;

    bytes_max = params->cache_size + 1;
    bytes_inuse = __btree_cache_bytes_inuse(params);
    if (bytes_inuse > (params->eviction_trigger * bytes_max) / 100)
        FLAG_SET(params->evict_flags, WT_CACHE_EVICT_CLEAN_HARD);
    else if (bytes_inuse > (params->eviction_target * bytes_max) / 100)
        FLAG_SET(params->evict_flags, WT_CACHE_EVICT_CLEAN);
}

static uint64_t
__btree_readgen_new(const cache_t *cache) {
    WT_params_t *params = (WT_params_t *)cache->eviction_params;

    return (params->read_gen + params->read_gen_oldest) / 2;
}

static inline void __evict_page_soon(cache_obj_t *obj) {
    obj->wt_page.read_gen = WT_READGEN_OLDEST;
}

#ifdef __cplusplus
}
#endif
