package agent

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

const (
	COMMON_EVENT   = 1
	CUSTOMED_EVENT = 2
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
	//ExtraMsg map[string]interface{} `json:"extra_msg"`
}

type FlumeCustomedEvent struct {
	Type int    `json:"type"`
	Path string `json:"path"`
	Body string `json:"body"`
}

func WriteCommonEvent(appkey, eventId string, timestamp int, bodyData []*CommonEventBody) error {
	if len(bodyData) <= 0 {
		return nil
	}
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
		Type:      COMMON_EVENT,
		Appkey:    appkey,
		EventId:   eventId,
		Timestamp: timestamp,
		Body:      string(bodyBuf),
	}
	buf, _ := json.Marshal(e)
	return a.WriteString(string(buf) + "\n")
}

func WriteCustomedEvent(path string, customEvent string) error {
	a, err := GetTaildirAgent()
	if err != nil {
		return err
	}
	e := &FlumeCustomedEvent{
		Type: CUSTOMED_EVENT,
		Path: path,
		Body: customEvent,
	}
	buf, _ := json.Marshal(e)
	return a.WriteString(string(buf) + "\n")
}
