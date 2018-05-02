package triblab

import (
    "sync"
    "trib"
    "trib/colon"
    "net/rpc"
    // "strconv"
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
    //iterately to find available back-end
    var bsc trib.Storage
    // var str string
    for index := originIndex; ; index = (index+1)%uint32(len(self.clients)) {
        tmpClient, err := rpc.DialHTTP("tcp", self.backs[index])
        if err != nil {
            continue
        }
        tmpClient.Close()
        // // check whether this backend has it 
        // err = self.clients[index].Get(strconv.Itoa(int(originIndex)), &str)
        // if err != nil {
        //     continue
        // }
        
        // if str != "true" {
        //     // fmt.Printf("%d machine does not have %d data\n", index, int(originIndex))
        //     continue
        // }

        bsc = &BinStorageClient{
            originIndex: int(originIndex),
            prefix: prefix,
            client: self.clients[index],
        }
        break
    }
    return bsc
}
