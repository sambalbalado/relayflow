package observability

import (
	"fmt"
	"io"
)

var DurationBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

type Snapshot struct {
	Steps              map[string]int64
	Workflows          map[string]int64
	Claims             int64
	Retries            int64
	Failures           int64
	Timeouts           int64
	Cancellations      int64
	LeaseExpirations   int64
	ExecutionBuckets   []int64
	ExecutionCount     int64
	ExecutionSumSecond float64
}

func WritePrometheus(writer io.Writer, snapshot Snapshot) error {
	write := func(format string, values ...any) error {
		_, err := fmt.Fprintf(writer, format, values...)
		return err
	}
	if err := write("# HELP relayflow_steps Current durable step count by state.\n# TYPE relayflow_steps gauge\n"); err != nil {
		return err
	}
	for _, status := range []string{"blocked", "ready", "running", "succeeded", "failed", "cancelled"} {
		if err := write("relayflow_steps{status=%q} %d\n", status, snapshot.Steps[status]); err != nil {
			return err
		}
	}
	if err := write("# HELP relayflow_workflows Current durable workflow count by state.\n# TYPE relayflow_workflows gauge\n"); err != nil {
		return err
	}
	for _, status := range []string{"pending", "running", "succeeded", "failed", "cancelled"} {
		if err := write("relayflow_workflows{status=%q} %d\n", status, snapshot.Workflows[status]); err != nil {
			return err
		}
	}
	counters := []struct {
		name  string
		help  string
		value int64
	}{
		{"relayflow_step_claims_total", "Durable step claim events.", snapshot.Claims},
		{"relayflow_step_retries_total", "Durable retry scheduling events.", snapshot.Retries},
		{"relayflow_step_failures_total", "Durable terminal step failure events.", snapshot.Failures},
		{"relayflow_step_timeouts_total", "Attempts terminated by execution deadlines.", snapshot.Timeouts},
		{"relayflow_workflow_cancellations_total", "Durable workflow cancellation events.", snapshot.Cancellations},
		{"relayflow_lease_expirations_total", "Durable lease expiration events.", snapshot.LeaseExpirations},
	}
	for _, counter := range counters {
		if err := write("# HELP %s %s\n# TYPE %s counter\n%s %d\n", counter.name, counter.help, counter.name, counter.name, counter.value); err != nil {
			return err
		}
	}
	if err := write("# HELP relayflow_step_execution_duration_seconds Completed attempt execution duration.\n# TYPE relayflow_step_execution_duration_seconds histogram\n"); err != nil {
		return err
	}
	for index, upper := range DurationBuckets {
		count := int64(0)
		if index < len(snapshot.ExecutionBuckets) {
			count = snapshot.ExecutionBuckets[index]
		}
		if err := write("relayflow_step_execution_duration_seconds_bucket{le=\"%g\"} %d\n", upper, count); err != nil {
			return err
		}
	}
	if err := write("relayflow_step_execution_duration_seconds_bucket{le=\"+Inf\"} %d\n", snapshot.ExecutionCount); err != nil {
		return err
	}
	if err := write("relayflow_step_execution_duration_seconds_sum %g\nrelayflow_step_execution_duration_seconds_count %d\n",
		snapshot.ExecutionSumSecond, snapshot.ExecutionCount); err != nil {
		return err
	}
	return nil
}
