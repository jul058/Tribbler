package triblab

import (
    "net"
    "net/rpc"
    "net/rpc/jsonrpc"
    "trib"
)

type client struct {
    ServerAddr string
    conn *rpc.Client
}
var _ trib.Storage = new(client)

func (self *client) Init() error {
    // connect to server
    conn, err := net.Dial("tcp", self.ServerAddr)

    if err != nil {
        return err
    }

    self.conn = rpc.NewClientWithCodec(jsonrpc.NewClientCodec(conn))
    return nil
}

func (self *client) Get(key string, value *string) error {
    if self.conn == nil {
        err := self.Init()
        if err != nil {
            return err
        }
    }
    // perform the call
    err := self.conn.Call("Storage.Get", key, value)
    if err == nil {
        return nil
    }
    // retry
    err = self.Init()
    if err != nil {
        return err
    }
    err = self.conn.Call("Storage.Get", key, value)
    if err != nil {
        return err
    }

    return nil
}

func (self *client) Set(kv *trib.KeyValue, succ *bool) error {
    if self.conn == nil {
        err := self.Init()
        if err != nil {
            return err
        }
    }
    // perform the call
    err := self.conn.Call("Storage.Set", kv, succ)
    if err == nil {
        return nil
    }
    // retry
    err = self.Init()
    if err != nil {
        return err
    }
    err = self.conn.Call("Storage.Set", kv, succ)
    if err != nil {
        return err
    }
    return nil
}

func (self *client) Keys(p *trib.Pattern, list *trib.List) error {
    if self.conn == nil {
        err := self.Init()
        if err != nil {
            return err
        }
    }
    // perform the call
    err := self.conn.Call("Storage.Keys", p, list)
    if err == nil {
        return nil
    }
    // retry
    err = self.Init()
    if err != nil {
        return err
    }
    err = self.conn.Call("Storage.Keys", p, list)
    if err != nil {
        return err
    }
    return nil
}

func (self *client) ListGet(key string, list *trib.List) error {
    if self.conn == nil {
        err := self.Init()
        if err != nil {
            return err
        }
    }
    // perform the call
    err := self.conn.Call("Storage.ListGet", key, list)
    if err == nil {
        return nil
    }
    // retry
    err = self.Init()
    if err != nil {
        return err
    }
    err = self.conn.Call("Storage.ListGet", key, list)
    if err != nil {
        return err
    }
    return nil
}

func (self *client) ListAppend(kv *trib.KeyValue, succ *bool) error {
    if self.conn == nil {
        err := self.Init()
        if err != nil {
            return err
        }
    }
    // perform the call
    err := self.conn.Call("Storage.ListAppend", kv, succ)
    if err == nil {
        return nil
    }
    // retry
    err = self.Init()
    if err != nil {
        return err
    }
    err = self.conn.Call("Storage.ListAppend", kv, succ)
    if err != nil {
        return err
    }
    return nil
}

func (self *client) ListRemove(kv *trib.KeyValue, n *int) error {
    if self.conn == nil {
        err := self.Init()
        if err != nil {
            return err
        }
    }
    // perform the call
    err := self.conn.Call("Storage.ListRemove", kv, n)
    if err == nil {
        return nil
    }
    // retry
    err = self.Init()
    if err != nil {
        return err
    }
    err = self.conn.Call("Storage.ListRemove", kv, n)
    if err != nil {
        return err
    }
    return nil
}

func (self *client) ListKeys(p *trib.Pattern, list *trib.List) error {
    if self.conn == nil {
        err := self.Init()
        if err != nil {
            return err
        }
    }
    // perform the call
    err := self.conn.Call("Storage.ListKeys", p, list)
    if err == nil {
        return nil
    }
    // retry
    err = self.Init()
    if err != nil {
        return err
    }
    err = self.conn.Call("Storage.ListKeys", p, list)
    if err != nil {
        return err
    }
    return nil
}

func (self *client) Clock(atLeast uint64, ret *uint64) error {
    if self.conn == nil {
        err := self.Init()
        if err != nil {
            return err
        }
    }
    // perform the call
    err := self.conn.Call("Storage.Clock", atLeast, ret)
    if err == nil {
        return nil
    }
    // retry
    err = self.Init()
    if err != nil {
        return err
    }
    err = self.conn.Call("Storage.Clock", atLeast, ret)
    if err != nil {
        return err
    }
    return nil
}
