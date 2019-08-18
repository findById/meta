package storage

import (
	"encoding/json"
	"github.com/findById/meta/storage/redis"
)

type Message struct {
	ProducerId    string `json:"producer_id"`    // 发送者Id
	ConsumerId    string `json:"consumer_id"`    // 接收者Id
	MessageId     uint16 `json:"message_id"`     // 消息Id
	MessageType   uint16 `json:"message_type"`   // 消息类型
	MessageStatus int32  `json:"message_status"` // 消息状态
	Topic         []byte `json:"topic"`          // 主题
	Payload       []byte `json:"payload"`        // 消息体
	ProduceTime   int64  `json:"produce_time"`   // 发送时间
	ConsumeTime   int64  `json:"consume_time"`   // 接收时间
}

type Storage interface {
	Profile(config map[string]string)
	Save(key string, value []byte) error
	Get(key string) ([]byte, error)
	Remove(key string) error
}

var (
	storage Storage
)

func init() {
	storage = new(redis.RedisStorage)
}

func Save(key string, message Message) error {
	buf, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return storage.Save(key, buf)
}

func Get(key string) (Message, error) {
	var msg Message
	buf, err := storage.Get(key)
	if err != nil {
		return msg, err
	}
	err = json.Unmarshal(buf, &msg)
	if err != nil {
		return msg, err
	}
	return msg, nil
}

func Remove(key string) error {
	return storage.Remove(key)
}
