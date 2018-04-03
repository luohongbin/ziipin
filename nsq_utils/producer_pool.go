package nsq_utils

import (
	"github.com/bitly/go-nsq"
	//"github.com/ziipin-server/niuhe"
	"log"
	"os"
	"time"
)

var mylog *log.Logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

func NewProducerPool(addrs []string) *ProducerPool {
	if len(addrs) == 0 {
		panic("rpc addrs can't be empty")
	}

	pool := &ProducerPool{
		recvChan: make(chan *ProducerCommonEvent, 2048),
	}
	for _, v := range addrs {
		go pool.process(v)
	}
	return pool
}

type ProducerCommonEvent struct {
	Topic string
	Buf   []byte
}

type ProducerPool struct {
	recvChan chan *ProducerCommonEvent
}

func (self *ProducerPool) process(addr string) {
	var (
		producer  *nsq.Producer
		err       error
		failTimes int
	)
connect:
	producer, err = nsq.NewProducer(addr, nsq.NewConfig())
	if err != nil {
		mylog.Println(err)
		if failTimes < 10 {
			failTimes++
		}
		time.Sleep(time.Second * time.Duration(failTimes))
		goto connect
	}
	failTimes = 0
	producer.SetLogger(mylog, nsq.LogLevelError)
	for {
		select {
		case e := <-self.recvChan:
			err := producer.Publish(e.Topic, e.Buf)
			if err != nil {
				mylog.Println(err)
				goto connect
			}
		}
	}
}

func (self *ProducerPool) Send(topic string, buf []byte) {
	self.recvChan <- &ProducerCommonEvent{
		Topic: topic,
		Buf:   buf,
	}
}
