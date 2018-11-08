package nsq_utils

import (
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"sync/atomic"
	"time"
	//"strconv"
)

type Publisher interface {
	Publish(topic string, buf []byte) error
}

const (
	MIDDLE_EVENT_TOPIC       = "middle_event"
	MIDDLE_EVENT_COUNT_TOPIC = "middle_event_count"
)

var count int64

type MiddleEvent struct {
	DistinctId string            `json:"distinct_id"`
	Project    string            `json:"project"`
	Type       string            `json:"type"`
	Timestamp  int64             `json:"timestamp"`
	EventName  string            `json:"event_name"`
	Properties map[string]string `json:"properties"`
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

func buildLocalSeq() string {
	atomic.AddInt64(&count, 1)
	return fmt.Sprintf("%d_%d", time.Now().Unix(), count)
}

func buildMiddleEvent(project, distinctId, eventName string, timestamp int64,
	properties map[string]string) (*MiddleEvent, error) {

	if err := middleEventCheck(project, distinctId, eventName, timestamp); err != nil {
		return nil, err
	}
	if len(properties) == 0 {
		return nil, fmt.Errorf("properties is not allowed empty")
	}
	if _, found := properties["seq"]; !found {
		properties["seq"] = buildLocalSeq()
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

func middleEventPublish(client *nsq.Producer, topic string, event *MiddleEvent) error {
	buf, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return client.Publish(topic, buf)
}

func MiddleEventCountPublish(
	client *nsq.Producer, project, distinctId, eventName string,
	timestamp int64, properties map[string]string) error {

	e, err := buildMiddleEvent(project, distinctId, eventName, timestamp, properties)
	if err != nil {
		return err
	}
	e.Type = "count"
	return middleEventPublish(client, MIDDLE_EVENT_COUNT_TOPIC, e)
}

func MiddleEventServerPublish(client *nsq.Producer, project, distinctId,
	eventName string, timestamp int64, properties map[string]string) error {

	e, err := buildMiddleEvent(project, distinctId, eventName, timestamp, properties)
	if err != nil {
		return err
	}
	e.Type = "server"
	return middleEventPublish(client, MIDDLE_EVENT_TOPIC, e)
}

func MiddleEventTrackPublish(client *nsq.Producer, project, distinctId,
	eventName string, timestamp int64, properties map[string]string) error {

	e, err := buildMiddleEvent(project, distinctId, eventName, timestamp, properties)
	if err != nil {
		return err
	}
	e.Type = "track"
	return middleEventPublish(client, MIDDLE_EVENT_TOPIC, e)
}
