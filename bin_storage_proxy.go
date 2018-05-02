package triblab

import (
    "sync"
    "trib"
    "trib/colon"
    "net/rpc"
    // "fmt"
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
    originIndex := hash % uint32(len(self.clients))
    index := originIndex
    //iterately to find available back-end
    var bsc trib.Storage
    for {
        tmpClient, err := rpc.DialHTTP("tcp", self.backs[index])
        if err == nil {
            // fmt.Printf("original: %d", originIndex)
            // fmt.Printf(", current: %d", index)
            // fmt.Println()
            tmpClient.Close()
            bsc = &BinStorageClient{
                originIndex: int(originIndex),
                prefix: prefix,
                client: self.clients[index],
            }
            break
        }
        index += 1
        index %= uint32(len(self.clients))
    }
    return bsc
}
