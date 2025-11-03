/*
Cmdstalk is a unix-process-based [beanstalkd][beanstalkd] queue broker.

Written in [Go][golang], cmdstalk uses the [kr/beanstalk][beanstalk]
library to interact with the [beanstalkd][beanstalkd] queue daemon.

Each job is passed as stdin to a new instance of the configured worker
command.  On `exit(0)` the job is deleted. On `exit(1)` (or any non-zero
status) the job is released with an exponential-backoff delay (releases^4),
up to 10 times.

If the worker has not finished by the time the job TTR is reached, the
worker is killed (SIGTERM, SIGKILL) and the job is allowed to time out.
When the job is subsequently reserved, the `timeouts: 1` will cause it to
be buried.

In this way, job workers can be arbitrary commands, and queue semantics are
reduced down to basic unix concepts of exit status and signals.
*/
package main

import (
	"time"

	"github.com/99designs/cmdstalk/broker"
	"github.com/99designs/cmdstalk/cli"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
)

// failsafeCircuitBreakerAdapter wraps a failsafe-go circuit breaker to implement broker.CircuitBreaker.
type failsafeCircuitBreakerAdapter struct {
	cb circuitbreaker.CircuitBreaker[any]
}

func (a *failsafeCircuitBreakerAdapter) TryAcquirePermit() bool {
	return a.cb.TryAcquirePermit()
}

func (a *failsafeCircuitBreakerAdapter) RemainingDelay() time.Duration {
	return a.cb.RemainingDelay()
}

func (a *failsafeCircuitBreakerAdapter) RecordSuccess() {
	a.cb.RecordSuccess()
}

func (a *failsafeCircuitBreakerAdapter) RecordFailure() {
	a.cb.RecordFailure()
}

func main() {
	opts := cli.MustParseFlags()

	// Create a circuit breaker creator function based on CLI options.
	var breakerCreator broker.CircuitBreakerCreator
	if opts.CircuitBreaker {
		// Create a real circuit breaker using failsafe-go with configured parameters.
		breakerCreator = func() broker.CircuitBreaker {
			cb := circuitbreaker.NewBuilder[any]().
				WithFailureThreshold(opts.CBFailureThreshold).
				WithDelay(opts.CBDelay).
				WithSuccessThreshold(opts.CBSuccessThreshold).
				Build()
			return &failsafeCircuitBreakerAdapter{cb: cb}
		}
	} else {
		// Return a no-op circuit breaker when disabled.
		breakerCreator = func() broker.CircuitBreaker {
			return broker.NewNoOpCircuitBreaker()
		}
	}

	bd := broker.NewBrokerDispatcher(opts.Address, opts.Cmd, opts.PerTube, breakerCreator)

	if opts.All {
		bd.RunAllTubes()
	} else {
		bd.RunTubes(opts.Tubes)
	}

	// TODO: wire up to SIGTERM handler etc.
	exitChan := make(chan bool)
	<-exitChan
}
