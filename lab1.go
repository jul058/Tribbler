package triblab

import (
    "net"
    "net/rpc"
    "net/rpc/jsonrpc"
    "trib"
)

// Creates an RPC client that connects to addr.
func NewClient(addr string) trib.Storage {
    return &client{ ServerAddr: addr, conn : nil}
}

// Serve as a backend based on the given configuration
func ServeBack(b *trib.BackConfig) error {
    server := rpc.NewServer()
    server.RegisterName("Storage", b.Store)
    server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

    lis, err := net.Listen("tcp", b.Addr)
    if err != nil {
        if b.Ready != nil {
            b.Ready <- false
        }
        return err
    }
    if b.Ready != nil {
        b.Ready <- true
    }
    for {
        conn, err := lis.Accept()
        if err != nil {
            if b.Ready != nil {
                b.Ready <- false
            }
            return err
        }

        go server.ServeCodec(jsonrpc.NewServerCodec(conn))
    }
    return nil
}
