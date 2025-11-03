/*
Package cli provides command line support for cmdstalk.
*/
package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// Options contains runtime configuration, and is generally the result of
// parsing command line flags.
type Options struct {

	// The beanstalkd TCP address.
	Address string

	// All == true means all tubes will be watched.
	All bool

	// The shell command to execute for each job.
	Cmd string

	// PerTube is the number of workers servicing each tube concurrently.
	PerTube uint64

	// The beanstalkd tubes to watch.
	Tubes TubeList

	// CircuitBreaker enables circuit breaker functionality.
	CircuitBreaker bool

	// CBFailureThreshold is the number of failures before opening the circuit breaker (default: 1).
	CBFailureThreshold uint

	// CBDelay is the delay when the circuit breaker is open (default: 1 minute).
	CBDelay time.Duration

	// CBSuccessThreshold is the number of successes needed to close the circuit breaker (default: 1).
	CBSuccessThreshold uint
}

// TubeList is a list of beanstalkd tube names.
type TubeList []string

// Calls ParseFlags(), os.Exit(1) on error.
func MustParseFlags() (o Options) {
	o, err := ParseFlags()
	if err != nil {
		flag.PrintDefaults()
		fmt.Println()
		fmt.Println(err)
		os.Exit(1)
	}
	return
}

// ParseFlags parses and validates CLI flags into an Options struct.
func ParseFlags() (o Options, err error) {
	o.Tubes = TubeList{"default"}

	flag.StringVar(&o.Address, "address", "127.0.0.1:11300", "beanstalkd TCP address.")
	flag.BoolVar(&o.All, "all", false, "Listen to all tubes, instead of -tubes=...")
	flag.StringVar(&o.Cmd, "cmd", "", "Command to run in worker.")
	flag.Uint64Var(&o.PerTube, "per-tube", 1, "Number of workers per tube.")
	flag.Var(&o.Tubes, "tubes", "Comma separated list of tubes.")
	flag.BoolVar(&o.CircuitBreaker, "circuit-breaker", false, "Enable circuit breaker functionality.")
	flag.UintVar(&o.CBFailureThreshold, "cb-failure-threshold", 1, "Number of failures before opening the circuit breaker.")
	flag.DurationVar(&o.CBDelay, "cb-delay", time.Minute, "Delay when the circuit breaker is open.")
	flag.UintVar(&o.CBSuccessThreshold, "cb-success-threshold", 1, "Number of successes needed to close the circuit breaker.")
	flag.Parse()

	err = validateOptions(o)

	return
}

func validateOptions(o Options) error {
	msgs := make([]string, 0)

	if o.Cmd == "" {
		msgs = append(msgs, "Command must not be empty.")
	}

	if o.Address == "" {
		msgs = append(msgs, "Address must not be empty.")
	}

	if len(msgs) == 0 {
		return nil
	} else {
		return errors.New(strings.Join(msgs, "\n"))
	}
}

// Set replaces the TubeList by parsing the comma-separated value string.
func (t *TubeList) Set(value string) error {
	list := strings.Split(value, ",")
	for i, value := range list {
		list[i] = value
	}
	*t = list
	return nil
}

func (t *TubeList) String() string {
	return fmt.Sprint(*t)
}
