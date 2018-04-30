package triblab

import (
    "strings"
    "trib"
    "trib/colon"
)

type BinStorageClient struct {
    prefix string
    client trib.Storage
}
var _ trib.Storage = new(BinStorageClient)

type appendList func([]string, string) []string

func (self *BinStorageClient) listHandler(list *trib.List, appendFunc appendList) {
    tmp := make([]string, len(list.L))
    copy(tmp, list.L)
    list.L = []string{}
    for _, element := range tmp {
        list.L = appendFunc(list.L, element)
    }
}

func (self *BinStorageClient) Get(key string, value *string) error {
    key = self.prefix + colon.Escape(key)
    return self.client.Get(key, value)
}

func (self *BinStorageClient) Set(kv *trib.KeyValue, succ *bool) error {
    myKv := &trib.KeyValue{ self.prefix + colon.Escape(kv.Key), colon.Escape(kv.Value) }
    return self.client.Set(myKv, succ)
}

func (self *BinStorageClient) Keys(p *trib.Pattern, list *trib.List) error {
    myP := &trib.Pattern{ self.prefix + colon.Escape(p.Prefix), colon.Escape(p.Suffix) }
    err := self.client.Keys(myP, list)
    if err != nil {
        return err
    }

    self.listHandler(list, func(l []string, e string) []string {return append(l, colon.Unescape(strings.TrimPrefix(e, self.prefix)))})
    return nil
}

func (self *BinStorageClient) ListGet(key string, list *trib.List) error {
    key = self.prefix + colon.Escape(key)
    err := self.client.ListGet(key, list)
    if err != nil {
        return err
    }

    self.listHandler(list, func(l []string, e string) []string {return append(l, colon.Unescape(e))})
    return nil
}

func (self *BinStorageClient) ListAppend(kv *trib.KeyValue, succ *bool) error {
    myKv := &trib.KeyValue{ self.prefix + colon.Escape(kv.Key), colon.Escape(kv.Value) }
    return self.client.ListAppend(myKv, succ)
}

func (self *BinStorageClient) ListRemove(kv *trib.KeyValue, n *int) error {
    myKv := &trib.KeyValue{ self.prefix + colon.Escape(kv.Key), colon.Escape(kv.Value) }
    return self.client.ListRemove(myKv, n)
}

func (self *BinStorageClient) ListKeys(p *trib.Pattern, list *trib.List) error {
    myP := &trib.Pattern{ self.prefix + colon.Escape(p.Prefix), colon.Escape(p.Suffix) }
    err := self.client.ListKeys(myP, list)
    if err != nil {
        return err
    }

    self.listHandler(list, func(l []string, e string) []string {return append(l, colon.Unescape(strings.TrimPrefix(e, self.prefix)))})
    return nil
}

func (self *BinStorageClient) Clock(atLeast uint64, ret *uint64) error {
    return self.client.Clock(atLeast, ret)
}
