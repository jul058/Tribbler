package triblab

import (
    "runtime"
    "runtime/debug"
    "strconv"
    "time"
    "testing"

    "trib"
)

func MyTribberConcurTest(t *testing.T, server1 trib.Server, server2 trib.Server) {
    runtime.GOMAXPROCS(2)

    ne := func(e error) {
        if e != nil {
            debug.PrintStack()
            t.Fatal(e)
        }
    }

    as := func(cond bool) {
        if !cond {
            debug.PrintStack()
            t.Fatal()
        }
    }

    signUp := func(done chan<- bool, errorChan chan<- error, server trib.Server, user string) {
        e := server.SignUp(user)
        done <- (e == nil)
        errorChan <- e
    }

    post := func(done chan<- bool, errorChan chan<- error, server trib.Server, user string, post string) {
        e := server.Post(user, post, 0)
        done <- (e == nil)
        errorChan <- e
    }

    nconcur := 2
    ntimes  := 10000
    done := make(chan bool, nconcur*2)
    errorChan := make(chan error, nconcur*2)
    go signUp(done, errorChan, server1, "user")
    as(<-done)

    t.Logf("Before first post at %s", time.Now())

    for i := 0; i < ntimes; i++ {
        cnt := 0
        for j := 0; j < nconcur; j++ {
            go post(done, errorChan, server1, "user", strconv.Itoa(i*1000 + j) + "a")
            go post(done, errorChan, server2, "user", strconv.Itoa(i*1000 + j) + "b")
        }
        for j := 0; j < nconcur*2; j++ {
            if <-done {
                cnt++
            }
            ne(<-errorChan)
        }
        as(cnt==2*nconcur)
    }

    t.Logf("After first post at %s", time.Now())

    tribbers, err := server1.Tribs("user")
    ne(err)
    as(len(tribbers) == trib.MaxTribFetch)

    t.Logf("After first fetch/Before second post at %s", time.Now())

    for _, element := range tribbers {
        t.Logf("User: %s\nMessage: %s\nTime: %s\nClock: %d\n", element.User, element.Message, element.Time, element.Clock)
    }

    for i := 0; i < ntimes; i++ {
        cnt := 0
        for j := 0; j < nconcur; j++ {
            go post(done, errorChan, server1, "user", strconv.Itoa(i*1000 + j) + "c")
            go post(done, errorChan, server2, "user", strconv.Itoa(i*1000 + j) + "d")
        }
        for j := 0; j < nconcur*2; j++ {
            if <-done {
                cnt++
            }
            ne(<-errorChan)
        }
        as(cnt==2*nconcur)
    }

    t.Logf("After second post/Before second fetch at %s", time.Now())

    tribbers, err = server1.Tribs("user")
    ne(err)
    as(len(tribbers) == trib.MaxTribFetch)

    t.Logf("After second fetch at %s", time.Now())
    
}
