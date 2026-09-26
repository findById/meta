package core

import (
	"strings"
	"sync"
)

type SubscriptionIndex struct {
	lock     sync.RWMutex
	exact    map[string]map[string]Subscriber
	wildcard *subscriptionNode
	count    int
}

type subscriptionNode struct {
	children map[string]*subscriptionNode
	clients  map[string]Subscriber
}

func NewSubscriptionIndex() *SubscriptionIndex {
	return &SubscriptionIndex{
		exact:    make(map[string]map[string]Subscriber),
		wildcard: newSubscriptionNode(),
	}
}

func newSubscriptionNode() *subscriptionNode {
	return &subscriptionNode{
		children: make(map[string]*subscriptionNode),
		clients:  make(map[string]Subscriber),
	}
}

func (i *SubscriptionIndex) Add(client *Client, filter string, qos byte) {
	if i == nil || client == nil || client.ID == "" || filter == "" {
		return
	}
	i.lock.Lock()
	defer i.lock.Unlock()
	entry := Subscriber{Client: client, Filter: filter, QoS: qos}
	if !strings.ContainsAny(filter, "+#") {
		if i.exact[filter] == nil {
			i.exact[filter] = make(map[string]Subscriber)
		}
		if _, ok := i.exact[filter][client.ID]; !ok {
			i.count++
		}
		i.exact[filter][client.ID] = entry
		return
	}
	node := i.wildcard
	for _, level := range strings.Split(filter, "/") {
		if node.children[level] == nil {
			node.children[level] = newSubscriptionNode()
		}
		node = node.children[level]
	}
	if _, ok := node.clients[client.ID]; !ok {
		i.count++
	}
	node.clients[client.ID] = entry
}

func (i *SubscriptionIndex) Remove(clientID string, filter string) {
	if i == nil || clientID == "" || filter == "" {
		return
	}
	i.lock.Lock()
	defer i.lock.Unlock()
	if !strings.ContainsAny(filter, "+#") {
		if clients := i.exact[filter]; clients != nil {
			if _, ok := clients[clientID]; ok {
				delete(clients, clientID)
				i.count--
			}
			if len(clients) == 0 {
				delete(i.exact, filter)
			}
		}
		return
	}
	node := i.wildcard
	for _, level := range strings.Split(filter, "/") {
		next := node.children[level]
		if next == nil {
			return
		}
		node = next
	}
	if _, ok := node.clients[clientID]; ok {
		delete(node.clients, clientID)
		i.count--
	}
}

func (i *SubscriptionIndex) Match(topic string) []Subscriber {
	if i == nil || topic == "" {
		return nil
	}
	i.lock.RLock()
	defer i.lock.RUnlock()
	seen := make(map[string]struct{})
	var result []Subscriber
	if clients := i.exact[topic]; clients != nil {
		for id, entry := range clients {
			result = append(result, entry)
			seen[id] = struct{}{}
		}
	}
	i.matchWildcard(i.wildcard, strings.Split(topic, "/"), 0, seen, &result)
	return result
}

func (i *SubscriptionIndex) Count() int {
	if i == nil {
		return 0
	}
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.count
}

func (i *SubscriptionIndex) matchWildcard(node *subscriptionNode, levels []string, index int, seen map[string]struct{}, result *[]Subscriber) {
	if node == nil {
		return
	}
	if hash := node.children["#"]; hash != nil {
		i.appendSubscribers(hash.clients, seen, result)
	}
	if index == len(levels) {
		i.appendSubscribers(node.clients, seen, result)
		return
	}
	if exact := node.children[levels[index]]; exact != nil {
		i.matchWildcard(exact, levels, index+1, seen, result)
	}
	if plus := node.children["+"]; plus != nil {
		i.matchWildcard(plus, levels, index+1, seen, result)
	}
}

func (i *SubscriptionIndex) appendSubscribers(clients map[string]Subscriber, seen map[string]struct{}, result *[]Subscriber) {
	for id, entry := range clients {
		if _, ok := seen[id]; ok {
			continue
		}
		*result = append(*result, entry)
		seen[id] = struct{}{}
	}
}
