package triblab

import (
    "time"
    "trib"
)

type Keeper struct {
    kc *trib.KeeperConfig
    backends []trib.Storage
}

func (self *Keeper) Init() {
    for _, addr := range self.kc.Backs {
        self.backends = append(self.backends, NewClient(addr))
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
            select {
                // https://stackoverflow.com/questions/16466320/is-there-a-way-to-do-repetitive-tasks-at-intervals-in-golang
                // this is better. Somehow equivalent to setInterval in js
                // time.Sleep will have timeskew eventually
                case <- ticker.C: 
                    for _, backend := range self.backends {
                        go func() {
                            err := backend.Clock(synClock, &ret)
                            if err != nil {
                                errorChannel <- err
                            }
                            synClockChannel <- ret
                        }()
                    }
                    for range self.backends {
                        if clock := <- synClockChannel; clock > synClock {
                            synClock = clock
                        }
                    }
                    // clock synchorizes within 2-3 seconds(depending on how you count)
                    // one back-end trivially satisfies sync
                    // two and above inducutively satisfy sync
                    synClock += 1
                    // time.Sleep(1 * time.Second)
            }
        }
    }()

    // will return when errorChannel is unblocked
    return <- errorChannel
}
