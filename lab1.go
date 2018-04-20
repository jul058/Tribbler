package triblab

import (
    "net"
    "net/http"
    "net/rpc"
    "trib"
)

// Creates an RPC client that connects to addr.
func NewClient(addr string) trib.Storage {
    return &client{ ServerAddr: addr, conn : nil }
}

// Serve as a backend based on the given configuration
func ServeBack(b *trib.BackConfig) error {
    server := rpc.NewServer()
    server.RegisterName("Storage", b.Store)

    lis, err := net.Listen("tcp", b.Addr)
    if err != nil {
        if b.Ready != nil {
            // b.Ready <- false
            go func(ch chan<- bool) { ch <- false } (b.Ready)
        }
        return err
    }
    if b.Ready != nil {
        go func(ch chan<- bool) { ch <- true } (b.Ready)
    }

    return http.Serve(lis, server)
}
