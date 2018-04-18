package triblab

import (
    "trib"
    "trib/colon"
)

type BinStorageProxy struct {
    backs []string
}
var _ trib.BinStorage = new(BinStorageProxy)

func (self *BinStorageProxy) Bin(name string) trib.Storage {
    hash := NewHash(name)
    return &BinStorageClient{ prefix: colon.Escape(name) + "::", client: NewClient(self.backs[hash % uint32(len(self.backs))]) }
}