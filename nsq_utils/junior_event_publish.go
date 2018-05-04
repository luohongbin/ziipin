package nsq_utils

import (
	"encoding/json"
	"fmt"
	"strconv"
)

const (
	JUNIOR_EVENT_TOPIC = "big_data_junior_event"
)

func check(project, distinctId, eventName string, timestamp int64) error {
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

func JuniorEventPublishCount(client Publisher, project, distinctId,
	eventName string, timestamp int64, dataMap map[string]string) error {

	if err := check(project, distinctId, eventName, timestamp); err != nil {
		return err
	}
	if len(dataMap) == 0 {
		return fmt.Errorf("dataMap is not allowed to be empty")
	}
	dataBuf, _ := json.Marshal(dataMap)
	e := &JuniorEvent{
		DistinctId: distinctId,
		Project:    project,
		Type:       "count",
		Timestamp:  timestamp,
		EventName:  eventName,
		Properties: map[string]string{
			"dataMap": string(dataBuf),
		},
	}
	buf, err := e.Marshal()
	if err != nil {
		return err
	}
	return client.Publish(JUNIOR_EVENT_TOPIC, buf)
}

func JuniorEventPublishCalc(client Publisher, project, distinctId,
	eventName string, timestamp int64, dataMap map[string]string, du int) error {
	if err := check(project, distinctId, eventName, timestamp); err != nil {
		return err
	}
	if len(dataMap) == 0 {
		return fmt.Errorf("dataMap is not allowed to be empty")
	}
	dataBuf, _ := json.Marshal(dataMap)
	e := &JuniorEvent{
		DistinctId: distinctId,
		Project:    project,
		Type:       "calc",
		Timestamp:  timestamp,
		EventName:  eventName,
		Properties: map[string]string{
			"dataMap": string(dataBuf),
			"du":      strconv.Itoa(du),
		},
	}
	buf, err := e.Marshal()
	if err != nil {
		return err
	}
	return client.Publish(JUNIOR_EVENT_TOPIC, buf)
}

func JuniorEventPublishTrack(client Publisher, project, distinctId,
	eventName string, timestamp int64, properties map[string]string) error {

	if err := check(project, distinctId, eventName, timestamp); err != nil {
		return err
	}
	if len(properties) == 0 {
		return fmt.Errorf("properties is not allowed to be empty")
	}
	e := &JuniorEvent{
		DistinctId: distinctId,
		Project:    project,
		Type:       "track",
		Timestamp:  timestamp,
		EventName:  eventName,
		Properties: properties,
	}
	buf, err := e.Marshal()
	if err != nil {
		return err
	}
	return client.Publish(JUNIOR_EVENT_TOPIC, buf)
}

func JuniorEventPublishServer(client Publisher, project, distinctId,
	eventName string, timestamp int64, properties map[string]string) error {

	if err := check(project, distinctId, eventName, timestamp); err != nil {
		return err
	}
	if len(properties) == 0 {
		return fmt.Errorf("properties is not allowed to be empty")
	}
	e := &JuniorEvent{
		DistinctId: distinctId,
		Project:    project,
		Type:       "server",
		Timestamp:  timestamp,
		EventName:  eventName,
		Properties: properties,
	}
	buf, err := e.Marshal()
	if err != nil {
		return err
	}
	return client.Publish(JUNIOR_EVENT_TOPIC, buf)
}
