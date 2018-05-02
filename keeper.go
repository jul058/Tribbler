package triblab

import (
    "sync"
    "time"
    "trib"
    "fmt"
)

const log_key = "LOG_KEY"

type Keeper struct {
    kc *trib.KeeperConfig
    backends []trib.Storage
    aliveBackends map[int] bool
    bitmap map[int] (map[int] bool)

    aliveBackendsLock sync.Mutex
    bitmapLock sync.Mutex
}

func (self *Keeper) Init() {
    self.aliveBackends = make(map[int] bool)
    self.bitmap = make(map[int] (map[int] bool))
    for index, addr := range self.kc.Backs {
        client := NewClient(addr)
        self.backends = append(self.backends, client)
        self.aliveBackends[index] = true
        self.bitmap[index] = make(map[int] bool)
    }
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
            for index := range self.backends {
                go func(i int) {
                    err := self.backends[i].Clock(synClock, &ret)
                    if err != nil {
                        // heartbeat fails
                        if self.aliveBackends[i] == true {
                            fmt.Println("about to crash, ", i)
                            self.crash(i)
                        }
                        synClockChannel <- 0
                    } else {
                        if self.aliveBackends[i] == false {
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
    replicator = replicator % len(self.bitmap)
    replicatee = replicatee % len(self.bitmap)
    //if use for, will cause infinite loop when only one alive back-end
    if replicator == replicatee {
        replicator += 1
        replicator %= len(self.backends)
    }
    backendLog := new(trib.List)
    successorLog := new(trib.List)
    backend := self.backends[replicatee]
    err := backend.ListGet(log_key + "_" + string(src), backendLog)
    if err != nil {
        // self crashed
        return
    }

    successor := self.backends[replicator]
    err = successor.ListGet(log_key + "_" + string(src), successorLog)
    // until it finds a alive successor
    for err != nil {
        // this successor has failed, try next one.
        replicator += 1
        replicator %= len(self.backends)
        successor = self.backends[replicator]
        err = successor.ListGet(log_key + "_" + string(src), successorLog)
    }


    // successor has self log
    //fmt.Printf("replicator: %d, replicatee: %d\n", replicator, replicatee)
    //fmt.Printf("successorLog: %s, backendLog: %s\n", successorLog.L, backendLog.L)
    self.bitmap[replicator][src] = true
    //fmt.Println("set true: [%d][%d]",replicator, src)
    for i := len(successorLog.L); i < len(backendLog.L); i+=1 {
        var succ bool
        err = successor.ListAppend(&trib.KeyValue{log_key + "_" + string(src), backendLog.L[i]}, &succ)
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
}


func (self *Keeper) replicate(errorChan chan<- error) {
    for range time.Tick(1 * time.Second) {
        for index, val := range self.aliveBackends {
            if val == true {
                self.bitmapLock.Lock()
                self.bitmap[index][index] = true
                // relicate self
                self.replicateLog(index, self.getSuccessor(index), index)
                // needs to check whether this backend is hosting other backend's log and that backend is dead. 
                // if so, then it needs to propogate the log to its successor. 
                for key, val := range self.bitmap[index] {
                    if val == true && self.aliveBackends[key] == false {
                        self.replicateLog(index, self.getSuccessor(index), key)
                    }
                }
                self.bitmapLock.Unlock()
            }
        }
    }
}

func (self *Keeper) crash(index int) {
    //fmt.Println("enter crash")
    self.aliveBackends[index] = false
    self.bitmapLock.Lock()
    defer self.bitmapLock.Unlock()
    logMap := self.bitmap[index]

    for key := range logMap {
        // if self has other backend's log
        if logMap[key] == true {
            if key == index {
                self.replicateLog(self.getSuccessor(index), self.getSuccessor(self.getSuccessor(index)), index)
            } else {
                self.replicateLog(self.getPredecessor(index), self.getSuccessor(index), key)
            }
            // label self no longer has that log
            self.bitmap[index][key] = false
        }
    }
}

func (self *Keeper) join(index int) {
    fmt.Println("Enter join")
    self.aliveBackends[index] = true
    //copy data belongs to this backend back to itself
    self.bitmapLock.Lock()
    defer self.bitmapLock.Unlock()
    for backupIndex := (index-1+len(self.bitmap))%len(self.bitmap); 
        backupIndex != index; 
        backupIndex = (backupIndex-1+len(self.bitmap))%len(self.bitmap) {
        if self.bitmap[backupIndex][index] == true {
            fmt.Println("replicatee: %d replicator: %d ", backupIndex, index)
            self.replicateLog(backupIndex, index, index)
            //stop this replicate
            self.bitmap[backupIndex][index] = false
            break
        }
    }
}

func (self *Keeper) getSuccessor(srcIndex int) int {
    for index := (srcIndex+1)%len(self.bitmap);
        index != srcIndex;
        index = (index+1)%len(self.bitmap) {
        if self.aliveBackends[index] == true {
            return index
        }
    }
    return (srcIndex+1)%len(self.bitmap)
}

func (self *Keeper) getPredecessor(srcIndex int) int {
    for index := (srcIndex-1+len(self.bitmap))%len(self.bitmap);
        index != srcIndex;
        index = (index-1+len(self.bitmap))%len(self.bitmap) {
        if self.aliveBackends[index] == true {
            return index
        }
    }
    return (srcIndex-1+len(self.bitmap))%len(self.bitmap)
}
