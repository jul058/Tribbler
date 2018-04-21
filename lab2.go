package triblab

import (
    "sync"
	"trib"
)

var (
    tribberServer trib.Server
    once sync.Once
)

func NewBinClient(backs []string) trib.BinStorage {
    return &BinStorageProxy{ backs: backs }
}

func ServeKeeper(kc *trib.KeeperConfig) error {
    return (&Keeper{ kc: kc, backends: []trib.Storage{} }).StartKeeper()
}

func NewFront(s trib.BinStorage) trib.Server {
    // Singleton 
    once.Do(func() {
        tribberServer = &Tribber{ binStorage: s }
    })
    return tribberServer
}
