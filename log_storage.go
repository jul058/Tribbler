package triblab
import {
	"encoding/json"
	"trib"
}

type Log_entry struct {
	opcode string
	kv trib.KeyValue
}

func LogToString(src *Log_entry) (string, error) {
	dst, err := json.Marshal(*src)
	if err != nil {
		return string(dst), err
	}

	return string(dst), nil
}

func StringToLog(src string) (*Log_entry, error) {
	var dst Log_entry
	err := json.Unmarshal([]byte(src), &dst)
	if err != nil {
		return &dst, err
	}

	return &dst, nil
}


