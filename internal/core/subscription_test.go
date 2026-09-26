package core

import "testing"

func TestSubscriptionIndexExactWildcardAndRemove(t *testing.T) {
	index := NewSubscriptionIndex()
	c1 := &Client{ID: "c1"}
	c2 := &Client{ID: "c2"}
	c3 := &Client{ID: "c3"}

	index.Add(c1, "device/1/status", 0)
	index.Add(c2, "device/+/status", 1)
	index.Add(c3, "device/#", 1)

	matches := index.Match("device/1/status")
	if len(matches) != 3 {
		t.Fatalf("matches = %d, want 3", len(matches))
	}
	if index.Count() != 3 {
		t.Fatalf("count = %d, want 3", index.Count())
	}

	index.Remove("c2", "device/+/status")
	matches = index.Match("device/1/status")
	if len(matches) != 2 {
		t.Fatalf("matches after remove = %d, want 2", len(matches))
	}
}
