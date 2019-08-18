package redis

import (
	"fmt"
	"qiniupkg.com/x/errors.v7"
)

var data = make(map[string][]byte, 0)

type RedisStorage struct {
}

func (item *RedisStorage) Profile(config map[string]string) {
	fmt.Println("=============Profile", config)
}

func (item *RedisStorage) Save(key string, value []byte) error {
	data[key] = value
	return nil
}

func (item *RedisStorage) Get(key string) ([]byte, error) {
	buf := data[key]
	if buf == nil {
		return nil, errors.New("empty")
	}
	return buf, nil
}

func (item *RedisStorage) Remove(key string) error {
	buf := data[key]
	if buf != nil {
		data[key] = nil
	}
	return nil
}
