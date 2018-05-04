package triblab

import (
    "time"
    "trib"
    "fmt"
    "strconv"
    "net"
    "net/http"
    "net/rpc"
    "sort"
    "sync"
    "trib/colon"
)

const log_key       = "LOG_KEY"
const alive_bin     = "ALIVE_BIN"
const bitmap_bin    = "BIT_BIN_"

type Keeper struct {
    kc *trib.KeeperConfig
    backends []trib.Storage
    binStorage trib.BinStorage
    cjLock sync.Mutex
}

func (self *Keeper) Init(stub_in string, stub_ret *string) error {
    if self.kc == nil {
	    return fmt.Errorf("Keeper Config. is nil.")
    }

    for _, b := range self.kc.Backs {
	    if b == "" {
            go func() {
    		    if self.kc.Ready != nil {
    			    self.kc.Ready <- false
    		    }
            }()
		    return fmt.Errorf("Invalid back-ends address for Keeper config.")
	    }
    }

    for _, k := range self.kc.Addrs {
	    if k == "" {
            go func() {
    		    if self.kc.Ready != nil {
    			    self.kc.Ready <- false
    		    }
            }()
		    return fmt.Errorf("Invalid Keeper address.")
	    }
    }


    self.kc.Id = time.Now().UnixNano() / int64(time.Microsecond)

    self.binStorage = NewBinClient(self.kc.Backs)
    for _, addr := range self.kc.Backs {
        client := NewClient(addr)
        self.backends = append(self.backends, client)

    }
    // Keeper struct initialized, starting Keeper Server
    // cannot use kc.Ready because there could be other waiting points beyond this function.
    var serverUp = make(chan bool, 1)
    var serverErr = make(chan error, 1)
    
    go func(self *Keeper, ready chan bool, errs chan error) error {
            kserver := rpc.NewServer()
            err := kserver.RegisterName("Keeper", self)
            if err != nil {
        	    fmt.Println("Could not register keeper server address %q ", self.kc.Addr())
		    if ready != nil {
			    ready <- false
		    }
		    if errs != nil {
			    errs <- err
		    }
		    return err
            }
        
            l, e := net.Listen("tcp", self.kc.Addr())
            if e != nil {
        	    fmt.Println("Could not open keeper address %q for listen.", self.kc.Addr())
		    if ready != nil {
			    ready <- false
		    }
		    if errs != nil {
			    errs <- e
		    }
        	    return e
            }
        
	    if ready != nil {
		    ready <- true
	    }
	    if errs != nil {
		    errs <- nil
	    }
        
            return http.Serve(l, kserver)
    }(self, serverUp, serverErr)

    serverReady := <-serverUp
    errS := <-serverErr

    if !serverReady {
	    if self.kc.Ready != nil {
		    self.kc.Ready <- false
	    }
	    return errS
    }

    // might need to delay self.kc.Ready <- true until entire StartKeeper is up.
    return nil
}

func (self *Keeper) initAliveAndBitmap () error {
    aliveBin := self.findBin(alive_bin)
    //set keeper's bin without calling bin()
    //for index, addr := range self.kc.Backs {
    for index, _ := range self.kc.Backs {
      alive := "true"
      bitmap := "true"

      //set alive flag
      var succ bool
      aliveBin.Set(&trib.KeyValue{strconv.Itoa(index), alive}, &succ)
      if !succ {
        return fmt.Errorf("Initialzie alive Bin failed")
      }
      //set bitmap flag
      bitMapBin := self.findBin(bitmap_bin+strconv.Itoa(index))
      bitMapBin.Set(&trib.KeyValue{strconv.Itoa(index), bitmap}, &succ)
      if !succ {
        return fmt.Errorf("Initialzie bitmap Bin failed")
      }

      //initialize added
      bitMapBin = self.findBin(bitmap_bin+strconv.Itoa((index+1)%len(self.kc.Backs)))
      bitMapBin.Set(&trib.KeyValue{strconv.Itoa(index), "true"}, &succ)
      if !succ {
        return fmt.Errorf("Initialzie bitmap Bin failed")
      }

    }
    return nil
}

func (self *Keeper) findBin(binName string) trib.Storage{
      //set alive flag
      binName = colon.Escape(binName + "::")
      binHash := NewHash(binName)
      originAliveIndex := binHash % uint32(len(self.kc.Backs))
      var bsc trib.Storage
      for aliveIndex := originAliveIndex; ; aliveIndex = (aliveIndex+1)%uint32(len(self.kc.Backs)) {
        tmpClient, err := rpc.DialHTTP("tcp", self.kc.Backs[aliveIndex])
        if err != nil {
          continue
        }
        tmpClient.Close()
        bsc = &BinStorageClient{
          originIndex: int(originAliveIndex),
          prefix: binName,
          client: self.backends[aliveIndex],
        }
        break
    }
    return bsc
}

func (self *Keeper) FindPrimary(stub_in string, pri_ret *int64) error {
	kidChan := make(chan int64)
	for _, kaddr := range self.kc.Addrs {
		go func(k string) {
			kclient := NewKeeperClient(k)

			var kid int64
			e := kclient.GetId(k, &kid)
			if e != nil {
				kidChan <- -1    // -1 as the maximum value to indicate error
			} else {
				kidChan <- kid 
			}
		}(kaddr)
	}

	var mink int64 
	mink = -1
	for _ = range self.kc.Addrs {
		k := <-kidChan
		if mink == -1 {
			mink = k
		} else if k != -1 && k < mink {
			mink = k
		}
	}

	*pri_ret = mink
	return nil
}

func (self *Keeper) GetBacks(stub string, backs *[]string) error {
	if self.kc == nil {
		return fmt.Errorf("Keeper not configured.")
	}

	*backs = self.kc.Backs
	return nil
}

func (self *Keeper) GetId(stub string, myId *int64) error {
	if self.kc == nil {
		return fmt.Errorf("Keeper not configured.")
	}

	*myId = self.kc.Id
	return nil
}

func (self *Keeper) retryGet(bin_key, key string) string {
    var val string
    err := self.binStorage.Bin(bin_key).Get(key, &val)
    if err != nil {
        // retry
        err = self.binStorage.Bin(bin_key).Get(key, &val)
    }
    if err != nil {
        return "error"
    }
    return val
}

func (self *Keeper) retrySet(bin_key string, kv *trib.KeyValue) {
    var succ bool
    err := self.binStorage.Bin(bin_key).Set(kv, &succ)
    if err != nil {
        err = self.binStorage.Bin(bin_key).Set(kv, &succ)
    }
    // fmt.Println(err)
}

func (self *Keeper) retryKeys(bin_key string, pattern *trib.Pattern) []string {
    ret := new(trib.List)
    err := self.binStorage.Bin(bin_key).Keys(pattern, ret)
    if err != nil {
        err = self.binStorage.Bin(bin_key).Keys(pattern, ret)
    }
    if err != nil {
        return []string{}
    }
    return ret.L
}

func (self *Keeper) StartKeeper(stub_in string, stub_ret *string) error {
    var stub string
    e := self.Init("", &stub)
    if e != nil {
	    // self.kc.Ready already filled with false if error
	    return e
    }
    var pri int64
    self.FindPrimary("", &pri) 
    if pri == self.kc.Id {
        e = self.initAliveAndBitmap()
    }

    // self.kc.Ready<-true not filled yet here, we need to delay until StartKeeper is fully prepared.

    errorChannel := make(chan error)
    synClockChannel := make(chan uint64)
    go func() {
        var ret uint64
        synClock := uint64(0)
        maxClock := uint64(0)
        for range time.Tick(1 * time.Second) {

    	    var pri int64
    	    self.FindPrimary("", &pri)
        	    if pri != self.kc.Id {
        		   continue
    	    }

    	    // do following only if self is primary, i.e. lowest Id
            for index := range self.backends {
                go func(i int) {
                    err := self.backends[i].Clock(synClock, &ret)
                    if err != nil {
                        // heartbeat fails
                        if self.retryGet(alive_bin, strconv.Itoa(i)) == "true" {
                            self.retrySet(alive_bin,
                              &trib.KeyValue{strconv.Itoa(i), "false"})
                            fmt.Println("about to crash, ", i)
                            self.crash(i)
                        }
                        synClockChannel <- 0
                    } else {
                        if self.retryGet(alive_bin, strconv.Itoa(i)) == "" {
                            fmt.Println("about to join, ", i)
                            self.retrySet(alive_bin,
                              &trib.KeyValue{strconv.Itoa(i), "false"})
                            self.join(i)
                        }
                        synClockChannel <- ret
                    }
                }(index)
            }

            for i := 0; i < len(self.backends); i+=1 {
                if clock := <- synClockChannel; clock > maxClock {
                    maxClock = clock
                }
            }
            if synClock >= maxClock {
                synClock += 1
            } else {
                synClock = maxClock
            }
        }
    }()

    // unblock any external wait 
    if self.kc.Ready != nil {
	    self.kc.Ready <- true
    }

    // boot up replication
    fmt.Printf("Keeper::Before replicate()\n")
    go self.replicate(errorChannel)
    // will return when errorChannel is unblocked
    e = <-errorChannel
    if e != nil {
            fmt.Println("StartKeeper replicate channel error ", e)
	    return e
    }

    return nil
}


func (self *Keeper) replicateLog(replicatee, replicator, src int) {
    replicator = replicator % len(self.backends)
    replicatee = replicatee % len(self.backends)
    //if use for, will cause infinite loop when only one alive back-end
    if replicator == replicatee {
        replicator += 1
        replicator %= len(self.backends)
    }
    backendLog := new(trib.List)
    successorLog := new(trib.List)
    backend := self.backends[replicatee]
    err := backend.ListGet(log_key + "_" + strconv.Itoa(src), backendLog)
    if err != nil {
        // self crashed
        return
    }

    successor := self.backends[replicator]
    err = successor.ListGet(log_key + "_" + strconv.Itoa(src), successorLog)
    // until it finds a alive successor
    for err != nil {
        // this successor has failed, try next one.
        replicator += 1
        replicator %= len(self.backends)
        successor = self.backends[replicator]
        err = successor.ListGet(log_key + "_" + strconv.Itoa(src), successorLog)
    }


    for i := len(successorLog.L); i < len(backendLog.L); i+=1 {
        var succ bool
        err = successor.ListAppend(&trib.KeyValue{log_key + "_" + strconv.Itoa(src), backendLog.L[i]}, &succ)
        if err != nil {
            // successor failure
            self.replicateLog(replicatee, self.getSuccessor(replicatee), src)
            return
        }
        // execute
        var logEntry *LogEntry
        logEntry, err = StringToLog(backendLog.L[i])
        if err != nil {
            // encoding failure
        }
        if logEntry.Opcode == "Set" {
            err = successor.Set(&logEntry.Kv, &succ)
        } else if logEntry.Opcode == "ListAppend" {
            err = successor.ListAppend(&logEntry.Kv, &succ)
        } else if logEntry.Opcode == "ListRemove" {
            var n int
            err = successor.ListRemove(&logEntry.Kv, &n)
        }
        if err != nil {
            // successor failure
            self.replicateLog(replicatee, self.getSuccessor(replicatee), src)
            return
        }
    }
    self.retrySet(bitmap_bin+strconv.Itoa(replicator), &trib.KeyValue{strconv.Itoa(src), "true"})
}


func (self *Keeper) replicate(errorChan chan<- error) {
    for range time.Tick(1 * time.Second) {
        for _, indexStr := range self.retryKeys(alive_bin, &trib.Pattern{"", ""}) {
            if self.retryGet(alive_bin, indexStr) == "true" {
                index, _ := strconv.Atoi(indexStr)
                self.retrySet(bitmap_bin+strconv.Itoa(index),
                  &trib.KeyValue{strconv.Itoa(index), "true"})
                // relicate self
                self.replicateLog(index, self.getSuccessor(index), index)
                // needs to check whether this backend is hosting other backend's log and that backend is dead. 
               // if so, and it is now holding the only ccpy, then it needs to propogate the log to its successor. 
                for _, keyStr :=
                  range self.retryKeys(bitmap_bin+indexStr, &trib.Pattern{"", ""}) {
                    key, _ := strconv.Atoi(keyStr)
                    copies := self.getNumberOfCopies(keyStr)
                    alive := self.retryGet(alive_bin, keyStr)
                    if alive != "true" && len(copies) == 1 {
/*
                        if index == 0 {
                          fmt.Printf("%d is sending %d to %d\n", index, key, self.getSuccessor(index))
                        }
*/
                        self.replicateLog(index, self.getSuccessor(index), key)
                    }
                }
            }
        }
    }
}

func (self *Keeper) crash(index int) {
    self.cjLock.Lock()
    defer self.cjLock.Unlock()
    self.retrySet(alive_bin, &trib.KeyValue{strconv.Itoa(index), ""})
    keys := self.retryKeys(bitmap_bin+strconv.Itoa(index), &trib.Pattern{"", ""})

    for _, keyStr := range keys {
         if index == 0 {
           fmt.Println("keys: ", keyStr)
         }
        key, _ := strconv.Atoi(keyStr)
/*
         self.retryGet(bitmap_bin+strconv.Itoa(index), keyStr)
         if key == index {
           self.replicateLog(self.getSuccessor(index), self.getSuccessor(self.getSuccessor(index)), index)
         } else {
             self.replicateLog(self.getPredecessor(index), self.getSuccessor(index), key)
         }
        // label self no longer has that log
*/
        self.retrySet(bitmap_bin+strconv.Itoa(index), &trib.KeyValue{strconv.Itoa(key), ""})
    }
}

func (self *Keeper) join(index int) {
    self.cjLock.Lock()
    defer self.cjLock.Unlock()
    fmt.Println("Enter join")
    successorMap := make([][]int, len(self.backends))
    for i := range successorMap {
        successorMap[i] = []int{}
    }

    for _, replicatorStr := range self.retryKeys(alive_bin, &trib.Pattern{"", ""}) {
      if self.retryGet(alive_bin, replicatorStr) == "true" {
          replicator, _ := strconv.Atoi(replicatorStr)
          // this replicatorStr is booking list
          bookKeep := self.retryKeys(bitmap_bin+replicatorStr, &trib.Pattern{"", ""})
          fmt.Printf("node %d is book keeping %s\n", replicator, bookKeep)
          // for each replicatee, record their replicator
          for _, replicateeStr := range bookKeep {
            replicatee, _ := strconv.Atoi(replicateeStr)
            successorMap[replicatee] = append(successorMap[replicatee], replicator)
          }
      }
    }

    for replicatee := range successorMap {
        numPairs := []*NumPair{}
        for replicator := range successorMap[replicatee] {
            numPairs = append(numPairs, 
                &NumPair{(successorMap[replicatee][replicator]+len(self.backends)-replicatee)%len(self.backends), 
                        successorMap[replicatee][replicator],})
        }
        numPairs = append(numPairs, 
            &NumPair{(index+len(self.backends)-replicatee)%len(self.backends), index})
        sort.Sort(ByKey(numPairs))
        if len(numPairs) >= 2 {
            self.replicateLog(numPairs[0].Right, numPairs[1].Right, replicatee)
            self.replicateLog(numPairs[1].Right, numPairs[0].Right, replicatee)
        }
            fmt.Printf("length: %d\n", len(numPairs))
        for replicator := 2; replicator < len(numPairs); replicator+=1 {
            // invalidate
            fmt.Printf("invalidating replicator %d on replicatee %d\n", numPairs[replicator].Right, replicatee)
            self.retrySet(bitmap_bin+strconv.Itoa(numPairs[replicator].Right), &trib.KeyValue{strconv.Itoa(replicatee), ""})
        }
    }
    self.retrySet(alive_bin, &trib.KeyValue{strconv.Itoa(index), "true"})

}

func (self *Keeper) getNumberOfCopies(index string) []int {
    copyIndex := []int{}
    for key := range self.backends {
        if self.retryGet(bitmap_bin+strconv.Itoa(key), index) == "true" {
            copyIndex = append(copyIndex, key)
        }
    }
    return copyIndex
}

func (self *Keeper) getSuccessor(srcIndex int) int {
    for index := (srcIndex+1)%len(self.backends);
        index != srcIndex;
        index = (index+1)%len(self.backends) {
        if self.retryGet(alive_bin, strconv.Itoa(index)) == "true" && 
            self.retryGet(bitmap_bin+strconv.Itoa(index), strconv.Itoa(srcIndex)) == "true" {
            return index
        }
    }
    return (srcIndex+1)%len(self.backends)
}

func (self *Keeper) getPredecessor(srcIndex int) int {
    for index := (srcIndex-1+len(self.backends))%len(self.backends);
        index != srcIndex;
        index = (index-1+len(self.backends))%len(self.backends) {
        if self.retryGet(alive_bin, strconv.Itoa(index)) == "true" && 
            self.retryGet(bitmap_bin+strconv.Itoa(index), strconv.Itoa(srcIndex)) == "true" {
            return index
        }
    }
    return (srcIndex-1+len(self.backends))%len(self.backends)
}
