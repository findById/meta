package core

import "testing"

func TestTopicMatch(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		topic  string
		want   bool
	}{
		{name: "exact", filter: "device/1/status", topic: "device/1/status", want: true},
		{name: "single level", filter: "device/+/status", topic: "device/1/status", want: true},
		{name: "single level does not cross level", filter: "device/+/status", topic: "device/1/meta/status", want: false},
		{name: "multi level", filter: "device/#", topic: "device/1/status", want: true},
		{name: "multi level zero", filter: "device/#", topic: "device", want: true},
		{name: "multi level must be last", filter: "device/#/status", topic: "device/1/status", want: false},
		{name: "different topic", filter: "device/1/status", topic: "device/2/status", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TopicMatch(tt.filter, tt.topic); got != tt.want {
				t.Fatalf("TopicMatch(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
			}
		})
	}
}
