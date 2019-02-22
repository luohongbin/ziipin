package nsq_utils

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"time"
	//"strconv"
)

var producer *nsq.Producer
var project string

func InitMiddleEventProducer(addr, pro string) error {
	if len(pro) <= 0 {
		return fmt.Errorf("project 不能是空")
	}
	var err error
	producer, err = nsq.NewProducer(addr, nsq.NewConfig())
	project = pro
	return err
}

func WriteMiddleEventCount(distinctId, eventName string, dataMap map[string]string) error {
	if producer == nil {
		return fmt.Errorf("先调用WriteMiddleEventCount,再使用此方法")
	}
	timestamp := time.Now().UnixNano() / 1000000
	return MiddleEventCountPublish(producer, project, distinctId, eventName, timestamp, dataMap)
}
