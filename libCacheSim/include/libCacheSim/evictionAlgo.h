#pragma once

#include "cache.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  cache_obj_t *q_head;
  cache_obj_t *q_tail;
} FIFO_params_t;

/* used by LFU related */
typedef struct {
  cache_obj_t *q_head;
  cache_obj_t *q_tail;
} LRU_params_t;

/* used by LFU related */
typedef struct freq_node {
  int64_t freq;
  cache_obj_t *first_obj;
  cache_obj_t *last_obj;
  uint32_t n_obj;
} freq_node_t;

typedef struct {
  cache_obj_t *q_head;
  cache_obj_t *q_tail;
  // clock uses one-bit counter
  int n_bit_counter;
  // max_freq = 1 << (n_bit_counter - 1)
  int max_freq;

  int64_t n_obj_rewritten;
  int64_t n_byte_rewritten;
} Clock_params_t;


typedef struct __wt_evict_queue {
    cache_obj_t* head;
    cache_obj_t* tail;
} wt_evict_queue_t;

/* A bucket holding evict queue with candidates within the bucket's range of read generations */
typedef struct __wt_evict_bucket {
	int upper_bound; /* lower bound is upper bound minus range step */
	wt_evict_queue_t evict_queue;       /* eviction candidates for this buffer */
} wt_evict_bucket_t;

#define WT_NUM_BUCKETS 100
typedef struct {
    uint32_t global_read_generation;
	wt_evict_bucket_t evict_buckets[WT_NUM_BUCKETS];
    cache_obj_t *BTree_root;
    cache_obj_t *evict_ref;
    uint64_t btree_inmem_bytes;
    uint64_t btree_internal_pages;
    uint64_t btree_total_pages;
    uint64_t cache_eviction_internal_pages_already_queued;
    uint64_t cache_eviction_internal_pages_queued;
    uint64_t cache_eviction_internal_pages_seen;
    uint64_t cache_eviction_pages_already_queued;
    uint64_t cache_eviction_pages_queued;
    uint64_t cache_eviction_pages_queued_post_lru;

    uint64_t cache_eviction_pages_seen;
    uint64_t cache_eviction_queue_empty;
    uint64_t cache_eviction_queue_not_empty;
    uint64_t cache_eviction_walk;
    uint64_t cache_eviction_walks_abandoned;
    uint64_t cache_eviction_walks_ended;
    uint64_t cache_eviction_walks_gave_up_ratio;
    uint64_t cache_eviction_walks_gave_up_no_targets;
    uint64_t cache_eviction_walk_passes;
    uint64_t cache_eviction_walks_started;
    uint64_t cache_eviction_walks_stopped;
    uint64_t cache_inmem_bytes;
    uint64_t cache_size;
    bool evict_aggressive;
    uint32_t evict_empty_score;
    uint32_t evict_flags;
    uint64_t evict_pass_gen;   /* total available evict slots */
    uint64_t evict_priority;
    uint32_t evict_slots;      /* total available evict slots */
    uint64_t eviction_target;  /* target percent cache full for eviction */
    uint64_t eviction_trigger; /* percent cache full when we begin eviction */
    u_int evict_start_type;
    uint64_t evict_walk_period;
    uint32_t evict_walk_progress;
    uint32_t evict_walk_target;
    uint64_t pages_evicted;
    uint64_t pages_inmem;
    uint64_t read_gen;
    uint64_t read_gen_oldest;
    uint64_t splitmempage;
} WT_params_t;


cache_t *ARC_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params);

cache_t *ARCv0_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params);

cache_t *Belady_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);

cache_t *BeladySize_init(const common_cache_params_t ccache_params,
                         const char *cache_specific_params);

cache_t *Cacheus_init(const common_cache_params_t ccache_params,
                      const char *cache_specific_params);

cache_t *Clock_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params);

cache_t *CR_LFU_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);

cache_t *FIFO_init(const common_cache_params_t ccache_params,
                   const char *cache_specific_params);

cache_t *GDSF_init(const common_cache_params_t ccache_params,
                   const char *cache_specific_params);

cache_t *Hyperbolic_init(const common_cache_params_t ccache_params,
                         const char *cache_specific_params);

cache_t *LeCaR_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params);

cache_t *LeCaRv0_init(const common_cache_params_t ccache_params,
                      const char *cache_specific_params);

cache_t *LFU_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params);

cache_t *LFUCpp_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);

cache_t *LFUDA_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params);

cache_t *LHD_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params);

cache_t *LRU_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params);

cache_t *LRUv0_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params);

cache_t *MRU_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params);

cache_t *Random_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);

cache_t *SLRU_init(const common_cache_params_t ccache_params,
                   const char *cache_specific_params);

cache_t *SLRUv0_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);

cache_t *SR_LRU_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);

cache_t *TwoQ_init(const common_cache_params_t ccache_params,
                   const char *cache_specific_params);

cache_t *LIRS_init(const common_cache_params_t ccache_params,
                   const char *cache_specific_params);

cache_t *Size_init(const common_cache_params_t ccache_params,
                   const char *cache_specific_params);

cache_t *WTinyLFU_init(const common_cache_params_t ccache_params,
                       const char *cache_specific_params);

cache_t *FIFO_Merge_init(const common_cache_params_t ccache_params,
                         const char *cache_specific_params);

cache_t *FIFO_Reinsertion_init(const common_cache_params_t ccache_params,
                               const char *cache_specific_params);

cache_t *flashProb_init(const common_cache_params_t ccache_params,
                        const char *cache_specific_params);

cache_t *LRU_Prob_init(const common_cache_params_t ccache_params,
                       const char *cache_specific_params);
cache_t *nop_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params);

cache_t *QDLP_init(const common_cache_params_t ccache_params,
                   const char *cache_specific_params);

cache_t *S3LRU_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params);

cache_t *S3FIFO_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);

cache_t *S3FIFOd_init(const common_cache_params_t ccache_params,
                      const char *cache_specific_params);

cache_t *Sieve_Belady_init(const common_cache_params_t ccache_params,
                           const char *cache_specific_params);

cache_t *LRU_Belady_init(const common_cache_params_t ccache_params,
                         const char *cache_specific_params);

cache_t *FIFO_Belady_init(const common_cache_params_t ccache_params,
                          const char *cache_specific_params);

cache_t *Sieve_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params);

cache_t *WT_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params);

#ifdef ENABLE_LRB
cache_t *LRB_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params);
#endif

#ifdef INCLUDE_PRIV
cache_t *SFIFOv0_init(const common_cache_params_t ccache_params,
                      const char *cache_specific_params);

cache_t *SFIFO_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params);

cache_t *LP_SFIFO_init(const common_cache_params_t ccache_params,
                       const char *cache_specific_params);

cache_t *LP_ARC_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);

cache_t *LP_TwoQ_init(const common_cache_params_t ccache_params,
                      const char *cache_specific_params);

cache_t *MyClock_init(const common_cache_params_t ccache_params,
                      const char *cache_specific_params);

cache_t *QDLPv0_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);

cache_t *S3FIFOdv2_init(const common_cache_params_t ccache_params,
                        const char *cache_specific_params);

cache_t *myMQv1_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);

cache_t *MClock_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);
#endif

#if defined(ENABLE_GLCACHE) && ENABLE_GLCACHE == 1

cache_t *GLCache_init(const common_cache_params_t ccache_params,
                      const char *cache_specific_params);

#endif

#ifdef __cplusplus
}
#endif
