package triblab

import (
    "sync"
    "trib"
    "trib/colon"
    "net/rpc"
)

type BinStorageProxy struct {
    backs []string
    clients []trib.Storage
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
    prefix := colon.Escape(name + "::")
    hash := NewHash(prefix)
    num := hash % uint32(len(self.clients))

    //iterately to find available back-end
    var bsc trib.Storage
    for true {
      _, err := rpc.DialHTTP("tcp", self.backs[num])
      if err == nil {
        bsc = &BinStorageClient{ 
                                prefix: prefix,
                                client: self.clients[num], 
                                id: num,
                            }
        break
      }
      num = num + 1
      if num >= uint32(len(self.clients)) {
        num = 0
      }
    }
    return bsc
}
