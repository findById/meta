package config

import (
	"encoding/json"
	"log"
	"os"
	"runtime"
	"fmt"
	"strconv"
)

const (
	VERSION       = "0.0.1"
	Host          = "host"
	Port          = "port"
	RedisServer   = "redis_server"
	RedisAuth     = "redis_auth"
	RedisPassword = "redis_password"
	RedisDB       = "redis_db"
)

var (
	Config    *Bean
	conf      map[string]interface{}
	GoVersion = runtime.Version()
)

type Bean struct {
	Host          string `json:"host"`
	Port          string `json:"port"`
	RedisServer   string `json:"redis_server"`
	RedisAuth     string `json:"redis_auth"`
	RedisPassword string `json:"redis_password"`
	RedisDB       int    `json:"redis_db"`
	DBHost        string `json:"db_host"`
	DBName        string `json:"db_name"`
	DBUsername    string `json:"db_username"`
	DBPassword    string `json:"db_password"`
}

func newConfig() {
	Config = &Bean{
		Host:          GetString("host", ""),
		Port:          GetString("port", ""),
		RedisServer:   GetString("redis_server", ""),
		RedisAuth:     GetString("redis_auth", ""),
		RedisPassword: GetString("redis_password", ""),
		RedisDB:       GetInt("redis_db", 0),
		DBHost:        GetString("db_host", ""),
		DBName:        GetString("db_name", ""),
		DBUsername:    GetString("db_username", ""),
		DBPassword:    GetString("db_password", ""),
	}
}

func ParseConfig(path string) error {
	file, err := os.Open(path)
	if err != nil {
		log.Println("read config", err)
		return err
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	dec.Decode(&conf)
	if err != nil {
		log.Println("decode config", err)
		return err
	}
	newConfig()
	return nil
}

func GetInt(key string, def int) int {
	v := conf[key]
	if v == nil {
		return def
	}
	res, err := strconv.ParseInt(fmt.Sprint(v), 10, 10)
	if err != nil {
		return def
	}
	return int(res)
}

func GetString(key, def string) string {
	v := conf[key]
	if v == nil {
		return def
	}
	return fmt.Sprint(v)
}

func GetBool(key string, def bool) bool {
	v := conf[key]
	if v == nil {
		return def
	}
	res, err := strconv.ParseBool(fmt.Sprint(v))
	if err != nil {
		return def
	}
	return res
}
