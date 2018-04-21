package triblab

import (
	"trib"
)

func NewBinClient(backs []string) trib.BinStorage {
    return &BinStorageProxy{ backs: backs }
}

func ServeKeeper(kc *trib.KeeperConfig) error {
    return (&Keeper{ kc: kc, backends: []trib.Storage{} }).StartKeeper()
}

func NewFront(s trib.BinStorage) trib.Server {
    return &Tribber{ binStorage: s }
}
