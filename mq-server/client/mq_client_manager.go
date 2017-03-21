package client

import "sync"

type ClientManager struct {
	ConnMap map[string]*MQClient
	Lock             sync.RWMutex
}

func NewClientManager() *ClientManager {
	cm := &ClientManager{
		ConnMap:make(map[string]*MQClient),
	}
	return cm;
}

func (this *ClientManager) AddClient(client *MQClient) {
	this.Lock.Lock();
	// defer this.Lock.Unlock();
	this.ConnMap[client.Id] = client;
	this.Lock.Unlock();
}

func (this *ClientManager) RemoveClient(id string) {
	this.Lock.Lock();
	// defer this.Lock.Unlock();
	c, ok := this.ConnMap[id];
	if ok {
		c.Close();
		delete(this.ConnMap, id);
	}
	this.Lock.Unlock();
}

func (this *ClientManager) GetClient(id string) *MQClient {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.ConnMap[id];
}

func (this *ClientManager) Size() int {
	return len(this.ConnMap);
}


func (this *ClientManager) CloneMap() []*MQClient {
	this.Lock.RLock()
	// defer this.Lock.RUnlock();
	clone := make([]*MQClient, len(this.ConnMap))
	i := 0;
	for _, v := range this.ConnMap {
		clone[i] = v;
		i++;
	}
	this.Lock.RUnlock();
	return clone
}
