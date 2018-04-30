package triblab

import (
    "fmt"
    "runtime"
    "runtime/debug"
    "strconv"
    "testing"

    "trib"
)

func MyCheckServerConcur(t *testing.T, server1 trib.Server, server2 trib.Server) {
    runtime.GOMAXPROCS(2)

    ne := func(e error) {
        if e != nil {
            debug.PrintStack()
            t.Fatal(e)
        }
    }

    er := func(e error) {
        if e == nil {
            debug.PrintStack()
            t.Fatal()
        }
    }

    as := func(cond bool) {
        if !cond {
            debug.PrintStack()
            t.Fatal()
        }
    }

    ne(server1.SignUp("user"))

    p := func(th, n int, done chan<- bool) {
        for i := 0; i < n; i++ {
            ne(server1.Post("user", strconv.Itoa(th*100+n), 0))
        }
        done <- true
    }

    nconcur := 4
    done := make(chan bool, nconcur)
    for i := 0; i < nconcur; i++ {
        go p(i, 10, done)
    }

    for i := 0; i < nconcur; i++ {
        <-done
    }

    ret, e := server1.Tribs("user")
    ne(e)
    as(len(ret) == 10*nconcur || len(ret) == trib.MaxTribFetch)

    ne(server1.SignUp("other"))
    fo := func(done chan<- bool, server trib.Server) {
        e := server.Follow("user", "other")
        done <- (e == nil)
    }

    unfo := func(done chan<- bool, server trib.Server) {
        e := server.Unfollow("user", "other")
        done <- (e == nil)
    }

    for i := 0; i < nconcur; i++ {
        go fo(done, server1)
        go fo(done, server1)
    }
    cnt := 0
    for i := 0; i < nconcur*2; i++ {
        if <-done {
            cnt++
        }
    }
    t.Logf("%d follows", cnt)
    // as(cnt == 1)

    er(server1.Follow("user", "other"))

    fos, e := server1.Following("user")
    ne(e)
    as(len(fos) == 1)
    as(fos[0] == "other")

    for i := 0; i < nconcur; i++ {
        go unfo(done, server1)
        go unfo(done, server1)
    }
    cnt = 0
    for i := 0; i < nconcur*2; i++ {
        if <-done {
            cnt++
        } 
    }
    t.Logf("%d unfollows", cnt)
    // as(cnt == 1)

    // time.Sleep(2 * time.Second)
    fos, e = server1.Following("user")
    ne(e)
    fmt.Printf("Following %d people", len(fos))
    as(len(fos) == 0)
}
