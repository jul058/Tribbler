package triblab

import (
    "sync"
    "trib"
    "trib/colon"
    "net/rpc"
    "strconv"
    "fmt"
    //"strings"
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
   fmt.Println("ender Bin() with name: ", name)
    self.Init()
    prefix := colon.Escape(name + "::")
    //fmt.Println(name)
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
        val := self.checkIfValid(index)
        if val {
  //       fmt.Println("index: ", index)
          bsc = &BinStorageClient{
            originIndex: int(originIndex),
            prefix: prefix,
            client: self.clients[index],
          }
          fmt.Println("index: ", index)
          fmt.Println()
          break
      }
    }
    return bsc
}

func (self *BinStorageProxy) checkIfValid(index uint32) bool {
  binName := bitmap_bin+strconv.Itoa(int(index))
  binName = colon.Escape(binName + "::")
//  fmt.Println("enter checkValid with binName: ", binName)
  binHash := NewHash(binName)
  originAliveIndex := binHash % uint32(len(self.clients))
  count := 0
  for aliveIndex := originAliveIndex;
      count < len(self.clients);
      aliveIndex = (aliveIndex+1)%uint32(len(self.clients)) {
    tmpClient, err := rpc.DialHTTP("tcp", self.backs[aliveIndex])
    if err != nil {
      continue
    }
    count+=1
    tmpClient.Close()
    bsc := &BinStorageClient{
      originIndex: int(originAliveIndex),
      prefix: binName,
      client: self.clients[aliveIndex],
    }
    var result string
    bsc.Get(strconv.Itoa(int(index)), &result)
/*
    fmt.Printf("index: %d", aliveIndex)
    fmt.Println()
    fmt.Printf("result: %v", result)
    fmt.Println()
    fmt.Println()
   */
    if result == "" {
      continue
    }
    resultBool, _ := strconv.ParseBool(result)
    return resultBool
  }
  return false
}
