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
    var ret uint64
    synClock := uint64(0)
    errorChannel := make(chan error)
    go func() {
        for {
            for _, backend := range self.backends {
                err := backend.Clock(synClock, &ret)
                if err != nil {
                    errorChannel <- err
                }
                if ret > synClock {
                    synClock = ret
                } else {
                    synClock += 1
                }
            }
            time.Sleep(1 * time.Second)
        }
    }()

    err := <- errorChannel
    return err
}