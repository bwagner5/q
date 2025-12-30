package sq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bwagner5/wq/pkg/sq"
	"github.com/stretchr/testify/require"
)

var (
	echoProcessor = func(ctx context.Context, input int) (int, error) {
		return input, nil
	}

	errProcessor = func(ctx context.Context, input int) (int, error) {
		return input, fmt.Errorf("error")
	}
)

func TestSQ(t *testing.T) {

	t.Run("Simple Work Queue", func(t *testing.T) {
		ctx := context.Background()
		queue := sq.New(2, 0, 5, echoProcessor)
		queue.Start(ctx)

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Second * 5)
		require.NoError(t, err)

		for i := 1; i <= 5; i++ {
			result, ok := queue.Result()
			require.Equal(t, true, ok)
			require.Equal(t, i, *result)
		}

		for range queue.Errors() {
			require.Fail(t, "should not have received any errors during processing")
		}

	})

	t.Run("Errors", func(t *testing.T) {
		ctx := context.Background()
		queue := sq.New(2, 1, 5, errProcessor)
		queue.Start(ctx)

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Second * 5)
		require.NoError(t, err)

		var errs []error
		for err := range queue.Errors() {
			errs = append(errs, err)
			require.Equal(t, fmt.Errorf("error"), err)
		}
		require.Len(t, errs, 5)
	})
}

func Benchmark(b *testing.B) {

	type QueueBenchmarks[T, R any] struct {
		name    string
		options sq.Options[T, R]
	}

	queueOpts := []QueueBenchmarks[int, int]{
		{
			name: "Default Queue w/ Results Queueing",
			options: sq.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 100,
				ResultsPolicy:    &sq.ResultsPolicyQueue,
				RetryPolicy:      &sq.DefaultRetryPolicy,
				ProcessorFunc:    echoProcessor,
			},
		},
		{
			name: "Queue w/ Result Dropping",
			options: sq.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 100,
				ResultsPolicy:    &sq.ResultsPolicyDrop,
				RetryPolicy:      &sq.DefaultRetryPolicy,
				ProcessorFunc:    echoProcessor,
			},
		},
		{
			name: "Queue w/ No Results",
			options: sq.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 0,
				RetryPolicy:      &sq.DefaultRetryPolicy,
				ProcessorFunc:    echoProcessor,
			},
		},
	}

	for _, opts := range queueOpts {
		b.Run(opts.name, func(b *testing.B) {
			queue := sq.NewFromOptions(opts.options)
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
		})
	}
}
