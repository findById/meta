package redis

import (
	"fmt"
	"log"
	"github.com/garyburd/redigo/redis"
	"time"
	"github.com/findById/doweidu/config"
)

const (
	KeyPublish    = "publish:msg:%v:%v"
)

var (
	RedisClient *redis.Pool
)

func InitRedis() {
	fmt.Println("conntcting to redis-server:", config.Config.RedisServer, config.Config.RedisDB)
	RedisClient = &redis.Pool{
		MaxIdle:     1,
		MaxActive:   10,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
			var conn redis.Conn
			var err error
			if config.Config.RedisAuth == "true" && config.Config.RedisPassword != "" {
				conn, err = redis.Dial("tcp", config.Config.RedisServer, redis.DialPassword(config.Config.RedisPassword))
			} else {
				conn, err = redis.Dial("tcp", config.Config.RedisServer)
			}
			if err != nil {
				fmt.Println("redis conntct failed:", err.Error())
				return nil, err
			}
			// 选择db
			conn.Do("SELECT", config.Config.RedisDB)
			return conn, nil
		},
	}
}

func reconnect() bool {
	stats := RedisClient.Stats()
	fmt.Printf("idle:%v, active:%v\n", stats.IdleCount, stats.ActiveCount)
	return true
}

func Set(key, value string, ex int64) error {
	if !reconnect() {
		return nil
	}
	conn := RedisClient.Get()
	defer conn.Close()
	var err error
	if ex == -1 {
		_, err = conn.Do("SET", key, value)
	} else {
		_, err = conn.Do("SET", key, value, "EX", ex)
	}
	if err != nil {
		fmt.Println("redis set ["+key+"] failed:", err)
		return err
	}
	return nil
}

func Get(key string) string {
	if !reconnect() {
		return ""
	}
	conn := RedisClient.Get()
	defer conn.Close()
	v, err := redis.String(conn.Do("GET", key))
	if err != nil {
		fmt.Println("redis get ["+key+"] failed:", err)
		return ""
	}
	return v
}

func Del(key string) {
	if !reconnect() {
		return
	}
	conn := RedisClient.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", key)
	if err != nil {
		fmt.Println("redis delelte ["+key+"] failed:", err)
	}
}

func Expire(key string, ex int64) {
	if !reconnect() {
		return
	}
	conn := RedisClient.Get()
	defer conn.Close()
	_, err := conn.Do("EXPIRE", key, ex)
	if err != nil {
		fmt.Println("redis expire ["+key+"] failed:", err)
	}
}

func Exists(key string) bool {
	if !reconnect() {
		return false
	}
	conn := RedisClient.Get()
	defer conn.Close()
	exists, err := redis.Bool(conn.Do("EXISTS", key))
	return err == nil && exists
}

func Size() int {
	if !reconnect() {
		return -1
	}
	conn := RedisClient.Get()
	defer conn.Close()
	size, err := conn.Do("KEYS", "*")
	if err != nil {
		log.Println(err)
		return 0
	}
	log.Println(size)
	return 0
}

func Flush() {
	conn := RedisClient.Get()
	defer conn.Close()
	conn.Flush()
}
