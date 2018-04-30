package triblab

import (
    // "encoding/json"
    "sync"
    "time"
    "trib"
)

const log_key = "LOG_KEY"

type Keeper struct {
    kc *trib.KeeperConfig
    backends []trib.Storage
    aliveBackends map[trib.Storage] bool
    bitmap map[int] (map[int] bool)

    aliveBackendsLock sync.Mutex
}

func (self *Keeper) Init() {
    self.aliveBackends = make(map[trib.Storage] bool)
    self.bitmap = make(map[int] (map[int] bool))
    for index, addr := range self.kc.Backs {
        client := NewClient(addr)
        self.backends = append(self.backends, client)
        self.aliveBackends[client] = true
        self.bitmap[index] = make(map[int] bool)
    }
}

func (self *Keeper) StartKeeper() error {
    self.Init()
    if self.kc.Ready != nil {
        self.kc.Ready <- true
    }
    ticker := time.NewTicker(1 * time.Second)
    errorChannel := make(chan error)
    synClockChannel := make(chan uint64)
    go func() {
        var ret uint64
        synClock := uint64(0)
        for {
            aliveNum := 0
            select {
                // https://stackoverflow.com/questions/16466320/is-there-a-way-to-do-repetitive-tasks-at-intervals-in-golang
                // this is better. Somehow equivalent to setInterval in js
                // time.Sleep will have timeskew eventually
                case <- ticker.C: 
                    for index, backend := range self.backends {
                        go func() {
                            err := backend.Clock(synClock, &ret)
                            if err != nil {
                                // errorChannel <- err
                                // heartbeat fails
                                if self.aliveBackends[backend] == true {
                                    go self.crash(backend, index)
                                }
                            } else {
                                aliveNum += 1
                                if self.aliveBackends[backend] == false {
                                    go self.join(backend, index)
                                }
                            }
                            synClockChannel <- ret
                        }()
                    }
                    for i := 0; i < aliveNum; i+=1 {
                        if clock := <- synClockChannel; clock > synClock {
                            synClock = clock
                        }
                    }
                    // clock synchorizes within 2-3 seconds(depending on how you count)
                    // one back-end trivially satisfies sync
                    // two and above inducutively satisfy sync
                    synClock += 1
            }
        }
    }()

    // boot up replication
    go self.replicate(errorChannel)
    // will return when errorChannel is unblocked
    return <-errorChannel
}


func (self *Keeper) replicateLog(replicatee, replicator int) {
    for replicator == replicatee {
        replicator += 1
        replicator %= len(self.backends)
    }
    backendLog := new(trib.List)
    successorLog := new(trib.List)
    backend := self.backends[replicatee]
    err := backend.ListGet(log_key, backendLog)
    if err != nil {
        // self crashed
        self.crash(self.backends[replicatee], replicatee)
        return
    }
    successor := self.backends[replicator]
    err = successor.ListGet(log_key, successorLog)
    // until it finds a alive successor
    for err != nil {
        self.crash(successor, replicator)
        // this successor has failed, try next one.
        replicator += 1
        replicator %= len(self.backends)
        successor = self.backends[replicator]
        err = successor.ListGet(log_key, successorLog)
    }


    // successor has self log
    self.bitmap[replicator][replicatee] = true
    for i := len(successorLog.L); i < len(backendLog.L); i+=1 {
        var succ bool
        err = successor.ListAppend(&trib.KeyValue{log_key + "_" + string(replicatee), backendLog.L[i]}, &succ)
        if err != nil {
            // successor failure
            self.crash(successor, replicator)
            // retry
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
            self.crash(successor, replicator)
            // retry
            self.replicateLog(replicatee, self.getSuccessor(replicatee))
            return
        }
    }
}


func (self *Keeper) replicate(errorChan chan<- error) {
    ticker := time.NewTicker(1 * time.Second)
    for {
        select {
            case <- ticker.C:
                // self.aliveBackendsLock.Lock()
                // for backend := range self.aliveBackends {
                for index := range self.backends {
                    self.replicateLog(index, self.getSuccessor(index))
                }
                // self.aliveBackendsLock.Unlock()
        }
    }
}

func (self *Keeper) crash(crashBackend trib.Storage, index int) {
    self.aliveBackends[crashBackend] = false
    logMap := self.bitmap[index]

    for key := range logMap {
        // if self has other backend's log
        if logMap[key] == true {
            self.replicateLog(self.getSuccessor(index), index+1)
            // label self no longer has that log
            self.bitmap[index][key] = false
        }
    }
}

func (self *Keeper) join(newBackend trib.Storage, index int) {
    //copy data belongs to this backend back to itself
    for backupIndex := (index - 1) % len(self.bitmap);
	backupIndex != index;
	backupIndex= ((backupIndex - 1) % len(self.bitmap)) {
      if self.bitmap[backupIndex][index] == true {
        self.replicateLog(backupIndex, index)
        //stop this replicate
        self.bitmap[backupIndex][index] = false
        break
      }
    }
    self.aliveBackends[newBackend] = true
    //set the new backup back-end for newBackend
    //go replicate(index, getSuccessor(index))

    //set the newBackend to be backup back-end for previous alive back-end
}

func (self *Keeper) getSuccessor(srcIndex int) int {
    for index := range self.backends {
        logMap := self.bitmap[index]
        if logMap[srcIndex] == true &&
        srcIndex != index {
            return index
        }
    }
    // should not happen, this means data loss
    return -1
}
