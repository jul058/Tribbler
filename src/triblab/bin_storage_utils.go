package triblab

import (
    "hash/fnv"
)

func NewHash(str string) uint32 {
    hash := fnv.New32a()
    hash.Write([]byte(str))
    return hash.Sum32()
}