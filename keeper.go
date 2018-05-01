package triblab

import (
    // "encoding/json"
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
        fmt.Println("address: ", index)
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
            aliveNum := 0
            for index, backend := range self.backends {
                go func() {
                    err := backend.Clock(synClock, &ret)
                    if err != nil {
                        // heartbeat fails
                        if self.aliveBackends[index] == true {
                            fmt.Println("about to crash, ", index)
                            self.crash(backend, index)
                        }
                        synClockChannel <- 0
                    } else {
                        aliveNum += 1
                        if self.aliveBackends[index] == false {
                            fmt.Println("about to join")
                            self.join(backend, index)
                        }
                        synClockChannel <- ret
                    }
                }()
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


func (self *Keeper) replicateLog(replicatee, replicator int) {
    replicator = replicator % len(self.bitmap)
    replicatee = replicatee % len(self.bitmap)
    for replicator == replicatee {
        replicator += 1
        replicator %= len(self.backends)
    }
    backendLog := new(trib.List)
    successorLog := new(trib.List)
    backend := self.backends[replicatee]
    err := backend.ListGet(log_key + "_" + string(replicatee), backendLog)
    if err != nil {
        // self crashed
        return
    }

    successor := self.backends[replicator]
    err = successor.ListGet(log_key + "_" + string(replicatee), successorLog)
    // until it finds a alive successor
    for err != nil {
        // this successor has failed, try next one.
        replicator += 1
        replicator %= len(self.backends)
        successor = self.backends[replicator]
        err = successor.ListGet(log_key + "_" + string(replicatee), successorLog)
    }


    // successor has self log
    fmt.Printf("replicator: %d, replicatee: %d\n", replicator, replicatee)
    fmt.Printf("successorLog: %s, backendLog: %s\n", successorLog.L, backendLog.L)
    self.bitmap[replicator][replicatee] = true
    for i := len(successorLog.L); i < len(backendLog.L); i+=1 {
        var succ bool
        err = successor.ListAppend(&trib.KeyValue{log_key + "_" + string(replicatee), backendLog.L[i]}, &succ)
        if err != nil {
            // successor failure
            self.replicateLog(replicatee, self.getSuccessor(replicatee))
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
            self.replicateLog(replicatee, self.getSuccessor(replicatee))
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
                self.bitmapLock.Unlock()
                self.replicateLog(index, self.getSuccessor(index))
            }
        }
    }
}

func (self *Keeper) crash(crashBackend trib.Storage, index int) {
    fmt.Println("enter crash")
    self.aliveBackends[index] = false
    self.bitmapLock.Lock()
    defer self.bitmapLock.Unlock()
    logMap := self.bitmap[index]

    for key := range logMap {
        // if self has other backend's log
        if logMap[key] == true {
            if key == index {
                fmt.Println("copy D to B")
                self.replicateLog(self.getSuccessor(index), index+1) 
            } else {
                fmt.Println("copy C to A")
                self.replicateLog(key, index+1)
            }
            // label self no longer has that log
            self.bitmap[index][key] = false
        }
    }
}

func (self *Keeper) join(newBackend trib.Storage, index int) {
    self.aliveBackends[index] = true
    //copy data belongs to this backend back to itself
    self.bitmapLock.Lock()
    defer self.bitmapLock.Unlock()
    for backupIndex := (index - 1) % len(self.bitmap);
	backupIndex != index;
	backupIndex = ((backupIndex - 1) % len(self.bitmap)) {
        if self.bitmap[backupIndex][index] == true {
            self.replicateLog(backupIndex, index)
            //stop this replicate
            self.bitmap[backupIndex][index] = false
            break
        }
    }
}

func (self *Keeper) getSuccessor(srcIndex int) int {
    for index := range self.backends {
        logMap := self.bitmap[index]
        if logMap[srcIndex] == true &&
        srcIndex != index {
            fmt.Println("src and successor", srcIndex, index)
            return index
        }
    }
    // 1. no successor, potentially data loss.
    // 2. first time, no replica for everyone. 
    return srcIndex+1
}
