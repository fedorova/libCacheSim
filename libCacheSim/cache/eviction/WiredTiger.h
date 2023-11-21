#pragma once

#include "../../dataStructure/map/map.h"
#include "../../include/libCacheSim/cache.h"

/* WiredTiger page types */
#define WT_ROOT -1
#define WT_INTERNAL 0
#define WT_LEAF 1

/* Node of a BTree. We support a single BTree inside the simulation. */
typedef struct {
    cache_obj_t *cache_obj;
    Map children;
} node_t;
