package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
	//导入mqtt包
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var f MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}
var fail_nums int = 0

func main() {
	//生成连接的客户端数
	c := flag.Uint64("n", 2, "client nums")
	flag.Parse()
	nums := int(*c)
	wg := sync.WaitGroup{}
	for i := 0; i < nums; i++ {
		wg.Add(1)
		time.Sleep(5 * time.Millisecond)
		go createTask(i, &wg)
	}
	wg.Wait()
}

func createTask(taskId int, wg *sync.WaitGroup) {
	defer wg.Done()
	opts := MQTT.NewClientOptions().AddBroker("tcp://127.0.0.1:1883").SetUsername("admin").SetPassword("admin")
	opts.SetClientID(fmt.Sprintf("go-simple-client:%d-%d", taskId, time.Now().Unix()))
	opts.SetDefaultPublishHandler(f)
	opts.SetConnectTimeout(time.Duration(60) * time.Second)
	//opts.SetProtocolVersion(4) // 3.1.1

	//创建连接
	c := MQTT.NewClient(opts)

	if token := c.Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
		fail_nums++
		fmt.Printf("taskId:%d,fail_nums:%d,error:%s \n", taskId, fail_nums, token.Error())
		return
	}

	c.Subscribe("test", 0, func(c MQTT.Client, msg MQTT.Message) {
		fmt.Println("msg:", fmt.Sprint(string(msg.Payload())))
	})

	//每隔5秒向topic发送一条消息
	i := 0
	for {
		i++
		time.Sleep(time.Duration(2) * time.Second)
		text := fmt.Sprintf("this is msg #%d! from task:%d", i, taskId)
		token := c.Publish("test", 1, false, text)
		token.Wait()
	}

	c.Disconnect(250)
	fmt.Println("task ok!!")
}