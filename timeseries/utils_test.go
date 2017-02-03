package timeseries

import (
	"testing"
)

func TestCalculateTimeSlotSize(t *testing.T) {
	tests := []struct {
		datapoints    int64
		startEpoch    int64
		endEpoch      int64
		minSlotSize   float64
		fixedSlotSize float64

		expected string
	}{
		{300, 100, 700, 0, 0, "2s"},
		{300, 100, 140, 300, 0, "5m"},
		{300, 100, 400, 0, 0, "1s"},
		{300, 100, 400, 100, 0, "2m"},
		{350, 100, 450, 350, 0, "6m"},
		{350, 100, 450, 350, 15, "15s"},
		{350, 100, 450, 350, 1567, "1567s"},
		{300, 100, 30100, 0, 0, "2m"},
		{300, 100, 140, 360, 0, "6m"},
		{300, 100, 30100, 180, 0, "3m"},
		{300, 100, 30100, 60, 0, "2m"},
		{150, 100, 3100, 30, 0, "30s"},
		{150, 100, 3100, 15, 0, "20s"},
	}

	for _, test := range tests {
		slot := CalculateTimeSlotSize(
			test.datapoints,
			test.startEpoch,
			test.endEpoch,
			test.minSlotSize,
			test.fixedSlotSize,
		)
		t.Logf("Test (duration: %d) (%+v)\n", test.endEpoch-test.startEpoch, test)
		if slot != test.expected {
			t.Errorf("Expected %s got %s", test.expected, slot)
		}
	}
}
