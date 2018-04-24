package triblab

import (
    "runtime"
    "runtime/debug"
    "strconv"
    "time"
    "testing"

    "trib"
)

func MyCacheConcurTest(t *testing.T, server1 trib.Server, server2 trib.Server) {
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

    nconcur := 2
    ntimes  := 10000
    done := make(chan bool, nconcur*2)
    errorChan := make(chan error, nconcur*2)
    
    for i := 0; i < ntimes; i++ {
        cnt := 0
        for j := 0; j < nconcur; j++ {
            go signUp(done, errorChan, server1, "user" + strconv.Itoa(i*1000+j)+"a")
            go signUp(done, errorChan, server2, "user" + strconv.Itoa(i*1000+j)+"b")
        }
        for j := 0; j < nconcur*2; j++ {
            if <- done {
                cnt++
            }
            // t.Logf("%s", <- errorChan)
            ne(<-errorChan)
        }
        // t.Logf("%d\n", cnt)
        as(cnt == nconcur*2)
    }

    listUser := func(done chan<- bool, errorChan chan<- error, userNumChan chan<- int, server trib.Server) {
        users, e := server.ListUsers()
        done <- (e == nil)
        errorChan <- e
        userNumChan <- len(users)
    }

    t.Logf("signUp finish at %s", time.Now())

    userNumChan := make(chan int, nconcur*2)
    for i := 0; i < ntimes/10; i++ {
        // t.Logf("%d, ", i)
        cnt := 0
        for j := 0; j < nconcur; j++ {
            go listUser(done, errorChan, userNumChan, server1)
            go listUser(done, errorChan, userNumChan, server2)
        }
        for j := 0; j < nconcur*2; j++ {
            if <- done {
                cnt++
            }
            ne(<-errorChan)
            as(20 == <-userNumChan)
        }
        as(cnt == nconcur*2)
    }

    t.Logf("listUser finish at %s", time.Now())
}
