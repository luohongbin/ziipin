package nsq_utils

import (
	"encoding/json"
	"fmt"
	//"strconv"
)

const (
	MIDDLE_EVENT_TOPIC = "big_data_middle_event"
)

type MiddleEvent struct {
	DistinctId string
	Project    string
	Type       string
	Timestamp  int64
	EventName  string
	Properties map[string]string
}

func middleEventCheck(project, distinctId, eventName string, timestamp int64) error {
	if project == "" {
		return fmt.Errorf("project不能为空")
	}
	if distinctId == "" {
		return fmt.Errorf("distinctId不能为空")
	}
	if eventName == "" {
		return fmt.Errorf("eventName不能为空")
	}
	if timestamp < 1525363200000 {
		return fmt.Errorf("timestamp应该是当前毫秒级时间戳")
	}
	return nil
}

func buildMiddleEvent(project, distinctId, eventName string, timestamp int64,
	properties map[string]string) (*MiddleEvent, error) {

	if err := middleEventCheck(project, distinctId, eventName, timestamp); err != nil {
		return nil, err
	}
	if len(properties) == 0 {
		return nil, fmt.Errorf("properties is not allowed empty")
	}

	e := &MiddleEvent{
		DistinctId: distinctId,
		Project:    project,
		Timestamp:  timestamp,
		EventName:  eventName,
		Properties: properties,
	}
	return e, nil
}

func middleEventPublish(client Publisher, event *MiddleEvent) error {
	buf, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return client.Publish(MIDDLE_EVENT_TOPIC, buf)
}

func MiddleEventCountPublish(
	client Publisher, project, distinctId, eventName string,
	timestamp int64, properties map[string]string) error {

	e, err := buildMiddleEvent(project, distinctId, eventName, timestamp, properties)
	if err != nil {
		return err
	}
	e.Type = "count"
	return middleEventPublish(client, e)
}

func MiddleEventServerPublish(client Publisher, project, distinctId,
	eventName string, timestamp int64, properties map[string]string) error {

	e, err := buildMiddleEvent(project, distinctId, eventName, timestamp, properties)
	if err != nil {
		return err
	}
	e.Type = "server"
	return middleEventPublish(client, e)
}

func MiddleEventTrackPublish(client Publisher, project, distinctId,
	eventName string, timestamp int64, properties map[string]string) error {

	e, err := buildMiddleEvent(project, distinctId, eventName, timestamp, properties)
	if err != nil {
		return err
	}
	e.Type = "track"
	return middleEventPublish(client, e)
}

