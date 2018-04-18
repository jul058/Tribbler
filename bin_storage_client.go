package triblab

import (
    "net/rpc"
    "strings"
    "trib"
    "trib/colon"
)

type BinStorageClient struct {
    prefix string
    addr string
    conn *rpc.Client
}
var _ trib.Storage = new(BinStorageClient)

func (self *BinStorageClient) Init() error {
    conn, err := net.Dial("tcp", self.addr)

    if err != nil {
        return err
    }

    self.conn := rpc.NewClientWithCodec(jsonrpc.NewClientCodec(conn))
    return nil
}

func (self *BinStorageClient) Get(key string, value *string) error {
    key = self.prefix + colon.Escape(key)
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

func (self *BinStorageClient) Set(kv *trib.KeyValue, succ *bool) error {
    kv.Key = self.prefix + colon.Escape(kv.Key)
    kv.Value = colon.Escape(kv.Value)
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

func (self *BinStorageClient) Keys(p *trib.Pattern, list *trib.List) error {
    p.Prefix = self.prefix + colon.Escape(p.Prefix)
    p.Suffix = colon.Escape(p.Suffix)
    err := self.conn.Call("Storage.Keys", p, list)
    if err != nil {
        // retry
        err = self.Init()
        if err != nil {
            return err
        }
        err = self.conn.Call("Storage.Keys", p, list)
        if err != nil {
            return err
        }
    }

    tmp := make([]string, len(list.L))
    copy(tmp, list.L)
    list.L = []string{}
    for _, element := range tmp {
        list.L = append(list.L, colon.Unescape(strings.TrimPrefix(element, self.prefix)))
    }

    return nil
}

func (self *BinStorageClient) ListGet(key string, list *trib.List) error {
    key = self.prefix + colon.Escape(key)
    err := self.conn.Call("Storage.ListGet", key, list)
    if err != nil {
        // retry
        err = self.Init()
        if err != nil {
            return err
        }
        err = self.conn.Call("Storage.ListGet", key, list)
        if err != nil {
            return err
        }
    }

    tmp := make([]string, len(list.L))
    copy(tmp, list.L)
    list.L = []string{}
    for _, element := range tmp {
        list.L = append(list.L, colon.Unescape(element))
    }

    return nil
}

func (self *BinStorageClient) ListAppend(kv *trib.KeyValue, succ *bool) error {
    kv.Key = self.prefix + colon.Escape(kv.Key)
    kv.Value = colon.Escape(kv.Value)
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

func (self *BinStorageClient) ListRemove(kv *trib.KeyValue, n *int) error {
    kv.Key = self.prefix + colon.Escape(kv.Key)
    kv.Value = colon.Escape(kv.Value)
    err := self.conn.Call("Storage.ListRemove", kv, n)
    if err != nil {
        return err
    }

    return nil
}

func (self *BinStorageClient) ListKeys(p *trib.Pattern, list *trib.List) error {
    p.Prefix = self.prefix + colon.Escape(p.Prefix)
    p.Suffix = colon.Escape(p.Suffix)
    err := self.conn.Call("Storage.ListKeys", p, list)
    if err != nil {
        return err
    }

    tmp := make([]string, len(list.L))
    copy(tmp, list.L)
    list.L = []string{}
    for _, element := range tmp {
        list.L = append(list.L, colon.Unescape(element))
    }
    return nil
}

func (self *BinStorageClient) Clock(atLeast uint64, ret *uint64) error {
    err := self.conn.Call("Storage.Clock", atLeast, ret)
    if err != nil {
        return err
    }

    return nil
}
