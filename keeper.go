package triblab

import (
    "time"
    "trib"
    "fmt"
    "strconv"
)

const log_key       = "LOG_KEY"
const alive_bin     = "ALIVE_BIN"
const bitmap_bin    = "BIT_BIN_"

type Keeper struct {
    kc *trib.KeeperConfig
    backends []trib.Storage
    binStorage trib.BinStorage
}

func (self *Keeper) Init() {
    self.kc.Id = time.Now().UnixNano() / int64(time.Microsecond)

    self.binStorage = NewBinClient(self.kc.Backs)
    for index, addr := range self.kc.Backs {
        client := NewClient(addr)
        self.backends = append(self.backends, client)


        // self.retrySet(alive_bin, &trib.KeyValue{strconv.Itoa(index), "true"})
        // self.retrySet(bitmap_bin+strconv.Itoa(index), &trib.KeyValue{strconv.Itoa(index), "true"})

        fmt.Printf("setting alive bin %s\n", strconv.Itoa(index))
    }
}

func (self *Keeper) FindPrimary() int64 {
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

	return mink
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

func (self *Keeper) StartKeeper() error {
    self.Init()
    if self.kc.Ready != nil {
        self.kc.Ready <- true
    }
    errorChannel := make(chan error)
    synClockChannel := make(chan uint64)
    go func() {
        var ret uint64
        synClock := uint64(0)
        for range time.Tick(1 * time.Second) {

    	    // pri := self.FindPrimary()
    	    // if pri != self.kc.Id {
    		   //  continue
    	    // }

    	    // do following only if self is primary, i.e. lowest Id
            for index := range self.backends {
                go func(i int) {
                    err := self.backends[i].Clock(synClock, &ret)
                    if err != nil {
                        // heartbeat fails
                        if self.retryGet(alive_bin, strconv.Itoa(i)) == "true" {
                            fmt.Println("about to crash, ", i)
                            self.crash(i)
                        }
                        synClockChannel <- 0
                    } else {
                        if self.retryGet(alive_bin, strconv.Itoa(i)) == "" {
                            fmt.Println("about to join, ", i)
                            self.join(i)
                        }
                        synClockChannel <- ret
                    }
                }(index)
            }
            for i := 0; i < len(self.backends); i+=1 {
                if clock := <- synClockChannel; clock > synClock {
                    synClock = clock
                }
            }
            synClock += 1
        }
    }()

    // boot up replication
    go self.replicate(errorChannel)
    // will return when errorChannel is unblocked
    return <-errorChannel
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


    // successor has self log
    // fmt.Printf("replicator: %d, replicatee: %d\n", replicator, replicatee)
    // fmt.Printf("successorLog: %s, backendLog: %s\n", successorLog.L, backendLog.L)
    // self.bitmap[replicator][src] = true

    //fmt.Println("set true: [%d][%d]",replicator, src)
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
            index, _ := strconv.Atoi(indexStr)
            // fmt.Printf("alive: %d, %d\n", idx, index)
            self.retrySet(bitmap_bin+strconv.Itoa(index), &trib.KeyValue{strconv.Itoa(index), "true"})
            // relicate self
            self.replicateLog(index, self.getSuccessor(index), index)
            // needs to check whether this backend is hosting other backend's log and that backend is dead. 
            // if so, then it needs to propogate the log to its successor. 
            for _, keyStr := range self.retryKeys(bitmap_bin+strconv.Itoa(index), &trib.Pattern{"", ""}) {
                key, _ := strconv.Atoi(keyStr)
                if self.retryGet(alive_bin, keyStr) == "" {
                    self.replicateLog(index, self.getSuccessor(index), key)
                }
            }
        }
    }
}

func (self *Keeper) crash(index int) {
    self.retrySet(alive_bin, &trib.KeyValue{strconv.Itoa(index), ""})
    keys := self.retryKeys(bitmap_bin+strconv.Itoa(index), &trib.Pattern{"", ""})

    for _, keyStr := range keys {
        key, _ := strconv.Atoi(keyStr)
        fmt.Printf("index: %d, key: %d\n", index, key)
        if key == index {
            self.replicateLog(self.getSuccessor(index), self.getSuccessor(self.getSuccessor(index)), index)
        } else {
            self.replicateLog(self.getPredecessor(index), self.getSuccessor(index), key)
        }
        // label self no longer has that log
        self.retrySet(bitmap_bin+strconv.Itoa(index), &trib.KeyValue{strconv.Itoa(key), ""})
    }
}

func (self *Keeper) join(index int) {
    fmt.Println("Enter join")
    self.retrySet(alive_bin, &trib.KeyValue{strconv.Itoa(index), "true"})
    //copy data belongs to this backend back to itself
    for backupIndex := (index-1+len(self.backends))%len(self.backends); 
        backupIndex != index; 
        backupIndex = (backupIndex-1+len(self.backends))%len(self.backends) {
        if self.retryGet(bitmap_bin+strconv.Itoa(backupIndex), strconv.Itoa(index)) == "true" {
            fmt.Println("replicatee: %d replicator: %d ", backupIndex, index)
            self.replicateLog(backupIndex, index, index)
            //stop this replicate
            self.retrySet(bitmap_bin+strconv.Itoa(backupIndex), &trib.KeyValue{strconv.Itoa(index), ""})
            break
        }
    }
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
