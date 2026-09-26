package core

import "strings"

func TopicMatch(filter, topic string) bool {
	if filter == topic {
		return true
	}
	if filter == "" || topic == "" {
		return false
	}

	filterLevels := strings.Split(filter, "/")
	topicLevels := strings.Split(topic, "/")
	for i, filterLevel := range filterLevels {
		if filterLevel == "#" {
			return i == len(filterLevels)-1
		}
		if i >= len(topicLevels) {
			return false
		}
		if filterLevel == "+" {
			continue
		}
		if filterLevel != topicLevels[i] {
			return false
		}
	}
	return len(filterLevels) == len(topicLevels)
}
