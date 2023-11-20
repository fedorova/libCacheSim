#include <cstdint>
#include <map>
#include <string>

#include "map.h"

typedef std::map<std::uint64_t, void*> CppMap;

extern "C" {

Map createMap() {
    return new CppMap();
}

int
insertPair(Map map, const uint64_t key, void* value)
{
    if (map) {
        CppMap* cppMap = static_cast<CppMap*>(map);
        (*cppMap)[key] = value;
        return (0);
    }
    return (-1);
}

void *
getValue(Map map, uint64_t key)
{
    if (map) {
        CppMap* cppMap = static_cast<CppMap*>(map);
        auto it = cppMap->find(key);
        if (it != cppMap->end()) {
            return it->second;
        }
    }
    return NULL;
}

int
removePair(Map map, uint64_t key)
{
    if (map) {
        CppMap* cppMap = static_cast<CppMap*>(map);
        return cppMap->erase(key);
    }
    return 0;
}

void deleteMap(Map map)
{
    if (map)
        delete static_cast<CppMap*>(map);
}

} // extern "C"
