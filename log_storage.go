package triblab
import (
	"encoding/json"
	"trib"
)

type LogEntry struct {
	Opcode string
	Kv trib.KeyValue
}

func LogToString(src *LogEntry) (string, error) {
	dst, err := json.Marshal(*src)
	if err != nil {
		return string(dst), err
	}

	return string(dst), nil
}

func StringToLog(src string) (*LogEntry, error) {
	var dst LogEntry
	err := json.Unmarshal([]byte(src), &dst)
	if err != nil {
		return &dst, err
	}

	return &dst, nil
}


