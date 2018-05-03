package triblab


type NumPair struct {
    Left int 
    Right int
}

type ByKey []*NumPair

func (s ByKey) Len() int {
    return len(s)
}

func (s ByKey) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}

func (s ByKey) Less(i, j int) bool {
    if s[i].Left != s[j].Right {
        return s[i].Left < s[j].Left
    } else {
        return s[i].Right < s[j].Right
    }
    return true
}