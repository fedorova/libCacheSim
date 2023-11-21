#ifndef MAP_H
#define MAP_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>


/* Define a handle for the C++ map to be used in C */
typedef void* Map;

/* Create a new map and return its handle */
Map createMap();

/* Insert a key-value pair into the map */
int insertPair(Map map, uint64_t key, void* value);

/* Get the value associated with a key from the map */
void * getValue(Map map, uint64_t);

/* Get the map size */
size_t getMapSize(Map map);

/* Get the key at a given index */
const uint64_t getKeyAtIndex(Map map, size_t index);

/* Get the value at a given index */
void* getValueAtIndex(Map map, size_t index);

/* Remove the pair from the map */
int removePair(Map map, uint64_t key);

/* Delete the map and free resources */
void deleteMap(Map map);

#ifdef __cplusplus
}
#endif

#endif /* MAP_H */
