package triblab

import (
    "encoding/json"
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
    for _, addr := range self.kc.Backs {
        client := NewClient(addr)
        self.backends = append(self.backends, client)
        self.aliveBackends[client] = true
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
                                    go self.join()
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
    // return <- errorChannel
    return <-errorChannel
}

func (self *Keeper) replicateLog(srcLog *trib.List, srcIndex int) {
    for {
        successorIndex := self.getSuccessor(srcIndex)
        successor := self.backends[successorIndex]
        var succ bool
        for _, logEntry := range srcLog.L {
            // shouldn't be log_key, b/c we are replicating someone else's data
            successor.ListAppend(&trib.KeyValue{log_key, logEntry}, &succ)
            if err != nill || succ == false {
                srcIndex = successorIndex
                // this means the backend has either crashed or, for some reason, failed
                continue
            }
        }
        // TODO: mark log key as 'complete'
        // mark the successor as true
        self.bitmap[srcIndex][successorIndex] = true

        // TODO: logEntry format has not been finalized yet
        // Once finalized, we need to insert into backends[i] data.

        // since we have find a good machine to replicate data
        break
    }
}

func (self *Keeper) replicate(errorChan chan<- error) {
    ticker := time.NewTicker(1 * time.Second)
    for {
        select {
            case <- ticker.C:
                // self.aliveBackendsLock.Lock()
                // for backend := range self.aliveBackends {
                for index, backend := range self.backends {
                    backendLog := new(trib.List)
                    successorLog := new(trib.List)
                    err := backend.ListGet(log_key, backendLog)
                    if err != nil {
                        // backend failure
                    }
                    successorIndex = self.getSuccessor(index)
                    successor = self.backends[successorIndex]
                    err = successor.ListGet(log_key, successorLog)
                    if err != nil {
                        // successor failure
                    }

                    for i := len(successorLog); i < len(backendLog); i+=1 {
                        var succ bool
                        // backend log, not self
                        err = successor.ListAppend(&trib.KeyValue{log_key + "_" + strconv.Itoa(index), backendLog[i]}, &succ)
                        if err != nil || succ == false {
                            // successor failure
                        }
                        // execute
                        var logEntry LogEntry
                        err = json.UnMarshal([]byte(backendLog[i]), &logEntry)
                        if err != nil {
                            // encoding failure
                        }
                        if logEntry.Opcode == "Set" {
                            err = successor.Set(&logEntry.KV, &succ)
                        } else if logEntry.Opcode == "ListAppend" {
                            err = successor.ListAppend(&logEntry.KV, &succ)
                        } else if logEntry.Opcode == "ListRemove" {
                            var n int
                            err = successor.ListRemove(&logEntry.KV, &n)
                        }
                    }
                }
                // self.aliveBackendsLock.Unlock()
        }
    }
}

func (self *Keeper) crash(crashBackend trib.Storage, index int) {
    self.aliveBackends[crashBackend] = false
    logMap := self.bitmap[index]
    for key := logMap {
        // if backend(index) has backend(key) log information
        if logMap[key] == true {

        }
    }
    
    go self.replicateLog()
}

func (self *Keeper) join(newBackend trib.Storage) {
    self.aliveBackends[newBackend] = true
}

func (self *Keeper) getSuccessor(srcIndex int) int {
    return srcIndex + 1
}
