package q_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bwagner5/q/pkg/q"
	"github.com/stretchr/testify/require"
)

func TestWQ(t *testing.T) {

	t.Run("Simple Work Queue", func(t *testing.T) {
		ctx := context.Background()
		queue := q.New(1, 0, 5, echoProcessor)
		queue.Start(ctx)

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Second * 5)
		require.NoError(t, err)

		for i := 1; i <= 5; i++ {
			result, ok := queue.Result()
			require.Equal(t, true, ok)
			require.Equal(t, i, result.Output)
		}
	})

	t.Run("Retries", func(t *testing.T) {
		ctx := context.Background()
		queue := q.New(1, 1, 5, errProcessor)
		queue.Start(ctx)

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Second * 5_000)
		require.NoError(t, err)

		for i := 1; i <= 5; i++ {
			err, ok := queue.Error()
			require.Equal(t, true, ok)
			require.Equal(t, 2, q.ResultErr[int, int](err).result.Metadata.Attempt())
			require.Equal(t, 1, result.Metadata.Retries())
			require.Equal(t, fmt.Errorf("error"), result.Err)
		}
	})
}

func Benchmark(b *testing.B) {

	type QueueBenchmarks[T, R any] struct {
		name    string
		options q.Options[T, R]
	}

	queueOpts := []QueueBenchmarks[int, int]{
		{
			name: "Default Queue w/ Results Queueing",
			options: q.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 100,
				ResultsPolicy:    &q.ResultsPolicyQueue,
				RetryPolicy:      &q.DefaultRetryPolicy,
				ProcessorFunc:    echoProcessor,
			},
		},
		{
			name: "Queue w/ Result Dropping",
			options: q.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 100,
				ResultsPolicy:    &q.ResultsPolicyDrop,
				RetryPolicy:      &q.DefaultRetryPolicy,
				ProcessorFunc:    echoProcessor,
			},
		},
		{
			name: "Queue w/ No Results",
			options: q.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 0,
				RetryPolicy:      &q.DefaultRetryPolicy,
				ProcessorFunc:    echoProcessor,
			},
		},
	}

	for _, opts := range queueOpts {
		b.Run(opts.name, func(b *testing.B) {
			queue := q.NewFromOptions(opts.options)
			queue.Start(b.Context())
			b.ResetTimer()

			b.RunParallel(func(p *testing.PB) {
				for p.Next() {
					queue.AddWithBackOff(1, 100*time.Microsecond, 3)
					_, ok := queue.Result()
					if opts.options.ResultsQueueSize > 0 {
						require.True(b, ok)
					}
				}
			})

			require.NoError(b, queue.Drain(time.Second*5))
			status := queue.Status()
			b.ReportMetric(float64(status.MinLatency.Nanoseconds()), "MinLatency(ns)")
			b.ReportMetric(float64(status.MaxLatency.Nanoseconds()), "MaxLatency(ns)")
			b.ReportMetric(float64(status.AvgLatency.Nanoseconds()), "AvgLatency(ns)")
		})
	}
}
