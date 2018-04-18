package triblab

import (
	"trib"
)

func NewBinClient(backs []string) trib.BinStorage {
    return &BinStorageProxy{ backs: backs }
}

func ServeKeeper(kc *trib.KeeperConfig) error {
	panic("todo")
}

func NewFront(s trib.BinStorage) trib.Server {
    return &Tribber{ binStorage: s }
}
