package utils

import "testing"

func TestRandomValue(t *testing.T) {
	for i := 1; i < 10; i++ {
		t.Logf("random value %v: %s", i, RandomValue(i))
	}
}
