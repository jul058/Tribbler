package triblab

import (
    "errors"
    "fmt"
    "math/rand"
    "sort"
    "strconv"
    "time"
    "trib"
)

type Tribber struct {
    binStorage trib.BinStorage
}
var _ trib.Server = new(Tribber)

const cache_bin_num         = 10
const users_list_key        = "USERS_LIST"
const users_list_cache      = "USERS_LIST_CACHE"
const existed_key           = "EXISTED"
const absent_key            = "ABSENT"
const tribble_list_key      = "TRIBBLE_LIST"

func (self *Tribber) checkUserExistence(bin_key, user string) (bool, error) {
    storage := self.binStorage.Bin(bin_key)
    var exist string
    err := storage.Get(user, &exist)
    if err != nil {
        return false, err
    }
    if exist != "" {
        return true, nil
    }
    return false, nil
}

func (self *Tribber) gc(user string) error {
    storage := self.binStorage.Bin(user)
    tribble_list := new(trib.List)
    err := storage.ListGet(tribble_list_key, tribble_list)
    ret := []*trib.Trib{}
    var tribbleStr string
    var removed int
    if err != nil {
        return err
    }
    var tribble *trib.Trib
    if len(tribble_list.L) > trib.MaxTribFetch {
        for _, element := range tribble_list.L {
            tribble, err = StringToTrib(element)
            if err != nil {
                return err
            }
            ret = append(ret, tribble)
        }
        sort.Sort(TribblerOrder(ret))
    
        for i := 0; i < len(ret)-trib.MaxTribFetch; i++ {
            tribbleStr, err = TribToString(ret[i])
            if err != nil {
                return err
            }
            err = storage.ListRemove(&trib.KeyValue{tribble_list_key, tribbleStr}, &removed)
            if err != nil {
                return err
            }
            if removed != 1 {
                return errors.New(fmt.Sprintf("GC::Fail to remove precisely one post"))
            }
        }
    }
    return nil
}

// Creates a user.
// Returns error when the username is invalid;
// returns error when the user already exists.
// Concurrent sign ups on the same user might both succeed with no error.
func (self *Tribber) SignUp(user string) error {
    if trib.IsValidUsername(user) == false {
        return errors.New(fmt.Sprintf("SignUp::Invalid username '%s'.", user))
    }
    if exist, err := self.checkUserExistence(users_list_key, user); err != nil {
        return err
    } else if exist == true {
        return errors.New(fmt.Sprintf("SignUp::User '%s' already exists.", user))
    }
    hash := NewHash(user)
    cacheStorage := self.binStorage.Bin(users_list_cache+strconv.Itoa(int(hash)%cache_bin_num))
    userList := new(trib.List)
    var succ bool
    err := cacheStorage.Keys(&trib.Pattern{"", ""}, userList)
    if err == nil {
        if len(userList.L) < trib.MinListUser {
            err = cacheStorage.Set(&trib.KeyValue{user, existed_key}, &succ)
        }
    }
    storage := self.binStorage.Bin(users_list_key)
    err = storage.Set(&trib.KeyValue{ user, existed_key }, &succ)
    if err != nil {
        return err
    }
    if succ == false {
        return errors.New(fmt.Sprintf("SignUp::Failed due to Storage error."))
    }
    return nil
}

// List 20 registered users.  When there are less than 20 users that
// signed up the service, all of them needs to be listed.  When there
// are more than 20 users that signed up the service, an arbitrary set
// of at lest 20 of them needs to be listed.
// The result should be sorted in alphabetical order.
func (self *Tribber) ListUsers() ([]string, error) {
    cache := self.binStorage.Bin(users_list_cache+strconv.Itoa(rand.Int()%cache_bin_num))
    storage := self.binStorage.Bin(users_list_key)
    userList := new(trib.List)
    var succ bool

    // check whether cache already has it
    err := cache.Keys(&trib.Pattern{ "", "" }, userList)
    if err != nil {
        return []string{}, err
    }
    if len(userList.L) >= trib.MinListUser {
        sort.Strings(userList.L)
        userList.L = userList.L[0:trib.MinListUser]
        return userList.L, nil
    }

    // cache does not have it, do a full look up
    err = storage.Keys(&trib.Pattern{ "", "" }, userList)
    if err != nil {
        return []string{}, err
    }
    sort.Strings(userList.L)
    if len(userList.L) > trib.MinListUser {
        userList.L = userList.L[0:trib.MinListUser]
        // update cache
        for _, user := range userList.L {
            err = cache.Set(&trib.KeyValue{ user, existed_key }, &succ)
            // ignore err and succ since this is caching, could write to log in the future
            if err != nil {
                return []string{}, err
            } else if succ != true {
                return []string{}, errors.New(fmt.Sprintf("ListUsers::Caching failed"))
            } 
        }
    }
    return userList.L, nil
}

// Post a tribble.  The clock is the maximum clock value this user has
// seen so far by reading tribbles or clock sync.
// Returns error when who does not exist;
// returns error when post is too long.
func (self *Tribber) Post(who, post string, clock uint64) error {
    if len(post) > trib.MaxTribLen {
        return errors.New(fmt.Sprintf("Post::'%s' has %d characters, too long.", len(post), post))
    }

    if exist, err := self.checkUserExistence(users_list_key, who); err != nil {
        return err
    } else if exist == false {
        return errors.New(fmt.Sprintf("Post::User '%s' does not exists.", who))
    }

    storage := self.binStorage.Bin(who)
    var newClock uint64
    err := storage.Clock(clock, &newClock)
    if err != nil {
        return err
    }

    var succ bool
    var newPost string
    if newClock == clock {
        newClock += 1
    }
    newPost, err = TribToString(&trib.Trib{who, post, time.Now(), newClock})
    if err != nil {
        return err
    }
    err = storage.ListAppend(&trib.KeyValue{tribble_list_key, newPost}, &succ)
    if err != nil {
        return err
    }
    if succ == false {
        return errors.New(fmt.Sprintf("Post::Failed due to Storage error."))
    }
    // 1/5 of the chance gc gets run
    if rand.Int() % 5 == 0 {
        go self.gc(who)
    }
    return nil
}

// List the tribs that a particular user posted.
// Returns error when user has not signed up.
func (self *Tribber) Tribs(user string) ([]*trib.Trib, error) {
    if exist, err := self.checkUserExistence(users_list_key, user); err != nil {
        return []*trib.Trib{}, err
    } else if exist == false {
        return []*trib.Trib{}, errors.New(fmt.Sprintf("Tribs::User '%s' does not exists.", user))
    }

    storage := self.binStorage.Bin(user)
    userTribbleList := new(trib.List)
    err := storage.ListGet(tribble_list_key, userTribbleList)
    if err != nil {
        return []*trib.Trib{}, err
    }
    ret := []*trib.Trib{}
    var tribbler *trib.Trib
    for _, element := range userTribbleList.L {
        tribbler, err = StringToTrib(element)
        if err != nil {
            return []*trib.Trib{}, err
        }
        ret = append(ret, tribbler)
    }

    sort.Sort(TribblerOrder(ret))
    if len(ret) > trib.MaxTribFetch {
        ret = ret[len(ret)-trib.MaxTribFetch:len(ret)]
        go self.gc(user)
    }
    return ret, nil
}

// Follow someone's timeline.
// Returns error when who == whom;
// returns error when who is already following whom;
// returns error when who is tryting to following
// more than trib.MaxFollowing users.
// returns error when who or whom has not signed up.
// Concurrent follows might both succeed without error.
// The count of following users might exceed trib.MaxFollowing=2000,
// if and only if the 2000'th user is generated by concurrent Follow()
// calls.
func (self *Tribber) Follow(who, whom string) error {
    if who == whom {
        return errors.New(fmt.Sprintf("Follow::Who and whom cannot be the same."))
    }
    if exist, err := self.checkUserExistence(users_list_key, who); err != nil {
        return err
    } else if exist == false {
        return errors.New(fmt.Sprintf("Follow::User '%s' does not exists.", who))
    }
    if exist, err := self.checkUserExistence(users_list_key, whom); err != nil {
        return err
    } else if exist == false {
        return errors.New(fmt.Sprintf("Tribs::User '%s' does not exists.", whom))
    }

    if exist, err := self.checkUserExistence(who, whom); err != nil {
        return err
    } else if exist == true {
        return errors.New(fmt.Sprintf("Follow::User '%s' is already following '%s'", who, whom))
    }
    storage := self.binStorage.Bin(who)
    currentFollow := new(trib.List)
    err := storage.Keys(&trib.Pattern{"", ""}, currentFollow)
    if err != nil {
        return err
    }
    if len(currentFollow.L) >= trib.MaxFollowing {
        return errors.New(fmt.Sprintf("Follow::Exceed maximum number of following."))
    }
    var succ bool
    err = storage.Set(&trib.KeyValue{whom, existed_key}, &succ)
    if err != nil {
        return err
    }
    if succ == false {
        return errors.New(fmt.Sprintf("Follow::Failed due to Storage error."))
    }
    return nil
}

// Unfollow someone's timeline.
// Returns error when who == whom.
// returns error when who is not following whom;
// returns error when who or whom has not signed up.
func (self *Tribber) Unfollow(who, whom string) error {
    if who == whom {
        return errors.New(fmt.Sprintf("Unfollow::Who and whom cannot be the same."))
    }
    if exist, err := self.checkUserExistence(users_list_key, who); err != nil {
        return errors.New(fmt.Sprintf("Unfollow::" + err.Error()))
    } else if exist == false {
        return errors.New(fmt.Sprintf("Unfollow::User '%s' does not exists.", who))
    }
    if exist, err := self.checkUserExistence(users_list_key, whom); err != nil {
        return errors.New(fmt.Sprintf("Unfollow::" + err.Error()))
    } else if exist == false {
        return errors.New(fmt.Sprintf("Unfollow::User '%s' does not exists.", whom))
    }

    if exist, err := self.checkUserExistence(who, whom); err != nil {
        return err
    } else if exist == false {
        return errors.New(fmt.Sprintf("Unfollow::User '%s' is not currently following '%s'", who, whom))
    }
    storage := self.binStorage.Bin(who)
    var succ bool
    err := storage.Set(&trib.KeyValue{whom, ""}, &succ)
    if err != nil {
        return err
    }
    if succ == false {
        return errors.New(fmt.Sprintf("Unfollow::Failed due to Storage error."))
    }
    return nil
}

// Returns true when who following whom.
// Returns error when who == whom.
// Returns error when who or whom has not signed up.
func (self *Tribber) IsFollowing(who, whom string) (bool, error) {
    if who == whom {
        return false, errors.New(fmt.Sprintf("IsFollowing::Who and whom cannot be the same."))
    }
    if exist, err := self.checkUserExistence(users_list_key, who); err != nil {
        return false, err
    } else if exist == false {
        return false, errors.New(fmt.Sprintf("IsFollowing::User '%s' does not exists.", who))
    }
    if exist, err := self.checkUserExistence(users_list_key, whom); err != nil {
        return false, err
    } else if exist == false {
        return false, errors.New(fmt.Sprintf("IsFollowing::User '%s' does not exists.", whom))
    }

    if exist, err := self.checkUserExistence(who, whom); err != nil {
        return false, err
    } else {
        return exist, nil
    }
}

// Returns the list of following users.
// Returns error when who has not signed up.
// The list have users more than trib.MaxFollowing=2000,
// if and only if the 2000'th user is generate d by concurrent Follow()
// calls.
func (self *Tribber) Following(who string) ([]string, error) {
    if exist, err := self.checkUserExistence(users_list_key, who); err != nil {
        return []string{}, err
    } else if exist == false {
        return []string{}, errors.New(fmt.Sprintf("Following::User '%s' does not exists.", who))
    }
    storage := self.binStorage.Bin(who)
    ret := new(trib.List)
    err := storage.Keys(&trib.Pattern{"", ""}, ret)

    if err != nil {
        return []string{}, err
    }
    return ret.L, nil
}

// List the tribs of someone's following users (including himself).
// Returns error when user has not signed up.
func (self *Tribber) Home(user string) ([]*trib.Trib, error) {
    if exist, err := self.checkUserExistence(users_list_key, user); err != nil {
        return []*trib.Trib{}, err
    } else if exist == false {
        return []*trib.Trib{}, errors.New(fmt.Sprintf("Home::User '%s' does not exist.", user))
    }

    storage := self.binStorage.Bin(user)
    follow_list := new(trib.List)
    err := storage.Keys(&trib.Pattern{"", ""}, follow_list)
    if err != nil {
        return []*trib.Trib{}, err
    }

    following_users := append(follow_list.L, user)
    ret := []*trib.Trib{}
    for _, name := range following_users {
        name_storage := self.binStorage.Bin(name)
        name_tribble_list := new(trib.List)
        err = name_storage.ListGet(tribble_list_key, name_tribble_list)
        if err != nil {
            return []*trib.Trib{}, err
        }
        for _, name_tribble := range name_tribble_list.L {
            tribble, err := StringToTrib(name_tribble)
            if err != nil {
                return []*trib.Trib{}, err
            }
            ret = append(ret, tribble)
        }
    }
    sort.Sort(TribblerOrder(ret))
    if len(ret) > trib.MaxTribFetch {
        ret = ret[len(ret)-trib.MaxTribFetch:len(ret)]
    }
    return ret, nil
}
    
