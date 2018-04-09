package nsq_utils

import (
	"fmt"
)

const (
	COMMON_EVENT_TOPIC   = "big_data_common_event"
	CUSTOMED_EVENT_TOPIC = "big_data_customed_event"
)

type Publisher interface {
	Publish(topic string, buf []byte) error
}

func CustomedEventPublish(client Publisher, path, body string) error {
	if len(path) == 0 || len(body) == 0 {
		return fmt.Errorf("path and body are required")
	}
	e := &CustomedEvent{
		Path: path,
		Body: body,
	}
	buf, err := e.Marshal()
	if err != nil {
		return err
	}
	return client.Publish(CUSTOMED_EVENT_TOPIC, buf)
}

func CommonEventPublish(client Publisher, appkey, eventId string, timestamp int, body map[string]string) error {
	if len(appkey) == 0 || len(eventId) == 0 || len(body) == 0 {
		return fmt.Errorf("appkey, eventId, body are required")
	}
	e := &CommonEvent{
		Appkey:    appkey,
		EventId:   eventId,
		Timestamp: int32(timestamp),
		DataMap:   body,
	}
	buf, err := e.Marshal()
	if err != nil {
		return err
	}
	return client.Publish(COMMON_EVENT_TOPIC, buf)
}
