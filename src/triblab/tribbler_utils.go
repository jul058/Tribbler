package triblab

import (
    "encoding/json"
    "trib"
)

type TribblerOrder []*trib.Trib

func TribToString(src *trib.Trib) (string, error) {
    dst, err := json.Marshal(*src)
    if err != nil {
        return string(dst), err
    }
    return string(dst), nil
}

func StringToTrib(src string) (*trib.Trib, error){
    var dst trib.Trib
    err := json.Unmarshal([]byte(src), &dst)
    if err != nil {
        return &dst, err
    }
    return &dst, nil
}

// https://gobyexample.com/sorting-by-functions
func (s TribblerOrder) Len() int {
    return len(s)
}

func (s TribblerOrder) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}

func (s TribblerOrder) Less(i, j int) bool {
    if s[i].Clock != s[j].Clock {
        return s[i].Clock < s[j].Clock
    }
    if s[i].Time.Equal(s[j].Time) == false {
        return s[i].Time.Before(s[j].Time)
    }
    if s[i].User != s[j].User {
        return s[i].User < s[j].User
    }
    if s[i].Message != s[j].Message {
        return s[i].Message < s[j].Message
    }
    return true
}
