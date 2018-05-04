package triblab_test

import (
	"testing"

	"os"
	"trib"
	"trib/entries"
	"trib/randaddr"
	"trib/store"
	"trib/tribtest"
	"triblab"
)

func startKeeper(t *testing.T, addr string) {
	readyk := make(chan bool)
	addrk := randaddr.Local()
	for addrk == addr {
		addrk = randaddr.Local()
	}

	go func() {
		e := triblab.ServeKeeper(&trib.KeeperConfig{
			Backs: []string{addr},
			Addrs: []string{addrk},
			This:  0,
			Id:    0,
			Ready: readyk,
		})
		if e != nil {
			t.Fatal(e)
		}
	}()

	if !<-readyk {
		t.Fatal("keeper not ready")
	}
}

func TestServer(t *testing.T) {
	if os.Getenv("TRIB_LAB") == "lab1" {
		t.SkipNow()
	}

	addr := randaddr.Local()
	ready := make(chan bool)
	go func() {
		e := entries.ServeBackSingle(addr, store.NewStorage(), ready)
		if e != nil {
			t.Fatal(e)
		}
	}()
	<-ready

	startKeeper(t, addr)

	server := entries.MakeFrontSingle(addr)

	tribtest.CheckServer(t, server)
}

func TestServerConcur(t *testing.T) {
	if os.Getenv("TRIB_LAB") == "lab1" {
		t.SkipNow()
	}

	addr := randaddr.Local()
	ready := make(chan bool)
	go func() {
		e := entries.ServeBackSingle(addr, store.NewStorage(), ready)
		if e != nil {
			t.Fatal(e)
		}
	}()

	<-ready

	startKeeper(t, addr)

	server1 := entries.MakeFrontSingle(addr)
	server2 := entries.MakeFrontSingle(addr)

	// tribtest.CheckServerConcur(t, server1)
	// triblab.MyCheckServerConcur(t, server1, server2)
	triblab.MyCacheConcurTest(t, server1, server2)
	//triblab.MyTribberConcurTest(t, server1, server2)
}
