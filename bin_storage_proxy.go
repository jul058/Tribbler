package triblab

import (
    "net"
    "net/rpc"
    "net/rpc/jsonrpc"
    "trib"
    "trib/colon"
)

type BinStorageProxy struct {
    backs []string
    conns []*rpc.Client
}
var _ trib.BinStorage = new(BinStorageProxy)

func (self *BinStorageProxy) Bin(name string) trib.Storage {
    hash := NewHash(name)
    return &BinStorageClient{ prefix: colon.Escape(name) + "::", addr: self.backs[hash % uint32(len(self.backs))], conn: nil }
}