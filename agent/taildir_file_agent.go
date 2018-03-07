package agent

import (
	"fmt"
)

var taildirAgent *FileWriteAgent

func GetTaildirAgent() (*FileWriteAgent, error) {
	if taildirAgent == nil {
		return taildirAgent, fmt.Errorf("run UseTaildirAgent first to init taildir")
	}
	return taildirAgent, nil
}

func UseTaildirAgent(path string) *FileWriteAgent {
	taildirAgent = NewFileWriteAgent(path).SetPrefix("taildir_")
	return taildirAgent
}

func WriteFLumeEvent(appkey, eventId, body string) error {
	a, err := GetTaildirAgent()
	if err != nil {
		return err
	}
	if eventId == "" {
		return fmt.Errorf("eventId is not allowed empty")
	}

	data := formatEvent(appkey, eventId, body)
	err = a.WriteString(data)
	return err
}

func WriteFlumeEvents(appkey string, dataMap map[string][]string) error {
	if len(dataMap) <= 0 {
		return nil
	}

	a, err := GetTaildirAgent()
	if err != nil {
		return err
	}

	var data string
	for eventId, datas := range dataMap {
		if eventId == "" {
			continue
		}
		for _, body := range datas {
			e := formatEvent(appkey, eventId, body)
			data += e
		}
	}
	err = a.WriteString(data)
	return err
}

func formatEvent(appkey, eventId, body string) string {
	return fmt.Sprintf("%s:%s:%s\n", appkey, eventId, body)
}
