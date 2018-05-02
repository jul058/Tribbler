package triblab

import (
    "net/rpc"
)

type KeeperClient struct {
    addr string
}

func (self *KeeperClient) GetBacks(stub string, backs *[]string) error {
    conn, e := rpc.DialHTTP("tcp", self.addr)
    if e != nil {
        return e
    }

    e = conn.Call("Keeper.GetBacks", stub, backs)
    if e != nil {
        conn.Close()
        return e
    }

    return conn.Close()
}

func (self *KeeperClient) GetId(stub string, myId *int64) error {
    conn, e := rpc.DialHTTP("tcp", self.addr)
    if e != nil {
        return e
    }

    e = conn.Call("Keeper.GetId", stub, myId)
    if e != nil {
        conn.Close()
        return e
    }

    return conn.Close()
}

func NewKeeperClient(addr string) *KeeperClient {
    return &KeeperClient{addr: addr}
}