package triblab

import (
    "sync"
    "trib"
    "trib/colon"
    "net/rpc"
    "strconv"
//    "fmt"
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
        // fmt.Printf("%s bin mapped on %d, original Index %d\n", name, index, originIndex)
/*
        if name == bitmap_bin + "1" {
          fmt.Println("index on ", index)
        }
*/
        break
      }
    }
    return bsc
}

func (self *BinStorageProxy) checkIfValid(index uint32) bool {
  aliveName := alive_bin
  escapeAliveName := colon.Escape(aliveName + "::")
  aliveHash := NewHash(escapeAliveName)
  binName := bitmap_bin+strconv.Itoa(int(index))
  escapeBinName := colon.Escape(binName + "::")
//  fmt.Println("enter checkValid with binName: ", binName)
  binHash := NewHash(escapeBinName)
  aliveFound := false
  binFound := false
  originAliveIndex := aliveHash % uint32(len(self.clients))
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
      prefix: escapeAliveName,
      client: self.clients[aliveIndex],
    }
    var result string
    bsc.Get(strconv.Itoa(int(index)), &result)

    if result != "true" {
      continue
    }
    aliveFound = true
  }

  originBitmapIndex := binHash % uint32(len(self.clients))
  count = 0
  for bitmapIndex := originBitmapIndex;
      count < len(self.clients);
      bitmapIndex = (bitmapIndex+1)%uint32(len(self.clients)) {
    tmpClient, err := rpc.DialHTTP("tcp", self.backs[bitmapIndex])
    if err != nil {
      continue
    }
    count+=1
    tmpClient.Close()
    bsc := &BinStorageClient{
      originIndex: int(originBitmapIndex),
      prefix: escapeBinName,
      client: self.clients[bitmapIndex],
    }
    var result string
    bsc.Get(strconv.Itoa(int(index)), &result)

    if result != "true" {
      continue
    }
    binFound = true
  }
  if binFound && aliveFound {
    return true
  }
  return false
}
