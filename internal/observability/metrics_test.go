package observability

import (
	"strings"
	"testing"
)

func TestWritePrometheus(t *testing.T) {
	snapshot := Snapshot{
		Steps: map[string]int64{"ready": 3}, Workflows: map[string]int64{"running": 2},
		Claims: 7, Retries: 2, ExecutionBuckets: make([]int64, len(DurationBuckets)),
		ExecutionCount: 4, ExecutionSumSecond: 1.25,
	}
	var output strings.Builder
	if err := WritePrometheus(&output, snapshot); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`relayflow_steps{status="ready"} 3`,
		`relayflow_workflows{status="running"} 2`,
		`relayflow_step_claims_total 7`,
		`relayflow_step_retries_total 2`,
		`relayflow_step_execution_duration_seconds_count 4`,
	} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("metrics output missing %q", expected)
		}
	}
}
