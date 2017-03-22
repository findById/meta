package client

import "sync"

type ClientManager struct {
	ConnMap map[string]*MQClient
	Lock    sync.RWMutex
}

func NewClientManager() *ClientManager {
	cm := &ClientManager{
		ConnMap: make(map[string]*MQClient),
	}
	return cm
}

func (this *ClientManager) AddClient(client *MQClient) {
	this.Lock.Lock()
	// defer this.Lock.Unlock();
	c, ok := this.ConnMap[client.Id]
	if ok {
		c.Close()
		delete(this.ConnMap, client.Id)
	}
	this.ConnMap[client.Id] = client
	this.Lock.Unlock()
}

func (this *ClientManager) RemoveClient(id string) {
	this.Lock.Lock()
	// defer this.Lock.Unlock();
	c, ok := this.ConnMap[id]
	if ok {
		c.Close()
		delete(this.ConnMap, id)
	}
	this.Lock.Unlock()
}

func (this *ClientManager) GetClient(id string) *MQClient {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.ConnMap[id]
}

func (this *ClientManager) Size() int {
	return len(this.ConnMap)
}

func (this *ClientManager) CloneMap() []*MQClient {
	this.Lock.RLock()
	// defer this.Lock.RUnlock();
	closedIds := make([]string, 0)

	clone := make([]*MQClient, len(this.ConnMap))
	i := 0
	for _, v := range this.ConnMap {
		if v.IsClosed {
			closedIds = append(closedIds, v.Id)
			continue
		}
		clone[i] = v
		i++
	}
	this.Lock.RUnlock()
	if len(closedIds) > 0 {
		this.remove(closedIds)
	}
	return clone
}

func (this *ClientManager) remove(ids []string) {
	this.Lock.RLock()
	for _, id := range ids {
		c, ok := this.ConnMap[id]
		if ok {
			c.Close()
			delete(this.ConnMap, id)
		}
	}
	this.Lock.RUnlock()
}
