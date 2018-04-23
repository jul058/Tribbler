package triblab

import (
    "sync"
    "trib"
    "trib/colon"
)

type BinStorageProxy struct {
    backs []string
    // proxy caches LIST_USER binStorageCleint
    userListStorageClientCache trib.Storage
    clients []trib.Storage
    //
    once sync.Once
}
var _ trib.BinStorage = new(BinStorageProxy)


func (self *BinStorageProxy) Init() {
    // create connections once per BinStorageProxy instance
    self.once.Do(func() {
        self.clients = []trib.Storage{}
        for _, addr := range self.backs {
            self.clients = append(self.clients, NewClient(addr))
        }
    })
}

func (self *BinStorageProxy) Bin(name string) trib.Storage {
    self.Init()
    hash := NewHash(name)
    if name == users_list_key {
        if self.userListStorageClientCache == nil {
            self.userListStorageClientCache = &BinStorageClient{ 
                                                                prefix: colon.Escape(name + "::"), 
                                                                client: self.clients[hash % uint32(len(self.clients))],
                                                            }
        }
        return self.userListStorageClientCache
    }
    return &BinStorageClient{ prefix: colon.Escape(name + "::"), client: self.clients[hash % uint32(len(self.clients))] }
}