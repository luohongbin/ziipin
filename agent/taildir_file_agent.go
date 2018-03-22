package agent

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
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

type FlumeCommonEvent struct {
	Type      int    `json:"type"`
	Appkey    string `json:"appkey"`
	EventId   string `json:"event_id"`
	Timestamp int    `json:"timestamp"`
	Body      string `json:"body"`
}

type CommonEventBody struct {
	SubEvent string `json:"sub_event"`
	Key      string `json:"key"`
	ExtraMsg string `json:"extra_msg"`
}

func WriteCommonEvent(appkey, eventId string, timestamp int, bodyData *CommonEventBody) error {
	a, err := GetTaildirAgent()
	if err != nil {
		return err
	}
	if eventId == "" || appkey == "" {
		return fmt.Errorf("appkey and eventId are not allowed empty")
	}
	now := int(time.Now().Unix())
	if math.Abs(float64(now-timestamp)) > 86400*7 {
		return fmt.Errorf("timestamp is ellegal: now=%d, timestamp=%d", now, timestamp)
	}
	bodyBuf, _ := json.Marshal(bodyData)
	e := &FlumeCommonEvent{
		Type:      1,
		Appkey:    appkey,
		EventId:   eventId,
		Timestamp: timestamp,
		Body:      string(bodyBuf),
	}
	buf, _ := json.Marshal(e)
	return a.WriteString(string(buf) + "\n")
}

// func WriteFLumeEvent(appkey, eventId, body string, timestamp int) error {
// 	a, err := GetTaildirAgent()
// 	if err != nil {
// 		return err
// 	}
// 	if eventId == "" {
// 		return fmt.Errorf("eventId is not allowed empty")
// 	}

// 	data := formatEvent(appkey, eventId, body, timestamp)
// 	err = a.WriteString(data)
// 	return err
// }

//func WriteFlumeEvents(appkey string, dataMap map[string][]string) error {
//	if len(dataMap) <= 0 {
//		return nil
//	}
//
//	a, err := GetTaildirAgent()
//	if err != nil {
//		return err
//	}
//
//	var data string
//	for eventId, datas := range dataMap {
//		if eventId == "" {
//			continue
//		}
//		for _, body := range datas {
//			e := formatEvent(appkey, eventId, body)
//			data += e
//		}
//	}
//	err = a.WriteString(data)
//	return err
//}

// func formatEvent(appkey, eventId, body string, timestamp int) string {
// 	return fmt.Sprintf("%s:%s:%d:%s\n", appkey, eventId, timestamp, body)
// }
