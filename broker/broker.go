/*
	Package broker reserves jobs from beanstalkd, spawns worker processes,
	and manages the interaction between the two.
*/
package broker

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/kr/beanstalk"
)

const (
	// ttrMargin compensates for beanstalkd's integer precision.
	// e.g. reserving a TTR=1 job will show time-left=0.
	// We need to set our SIGTERM timer to time-left + ttrMargin.
	ttrMargin = 1 * time.Second

	// deadlineSoonDelay defines a period to sleep between receiving
	// DEADLINE_SOON in response to reserve, and re-attempting the reserve.
	deadlineSoonDelay = 1 * time.Second
)

type Broker struct {

	// Address of the beanstalkd server.
	Address string

	// The shell command to execute for each job.
	Cmd string

	// Tube name this broker will service.
	Tube string

	log     *log.Logger
	results chan<- *JobResult
}

type JobResult struct {

	// Buried is true if the job was buried.
	Buried bool

	// Executed is true if the job command was executed (or attempted).
	Executed bool

	// ExitStatus of the command; 0 for success.
	ExitStatus int

	// JobId from beanstalkd.
	JobId uint64

	// Stdout of the command.
	Stdout []byte

	// TimedOut indicates the worker exceeded TTR for the job.
	// Note this is tracked by a timer, separately to beanstalkd.
	TimedOut bool

	// Error raised while attempting to handle the job.
	Error error
}

// New broker instance.
func New(address, tube string, cmd string, results chan<- *JobResult) (b Broker) {
	b.Address = address
	b.Tube = tube
	b.Cmd = cmd

	b.log = log.New(os.Stdout, fmt.Sprintf("[%s] ", tube), log.LstdFlags)
	b.results = results
	return
}

// reserve-with-timeout until there's a job or something panic-worthy.
func (b *Broker) mustReserveWithoutTimeout(ts *beanstalk.TubeSet) (id uint64, body []byte) {
	var err error
	for {
		id, body, err = ts.Reserve(1 * time.Hour)
		if err == nil {
			return
		} else if err.(beanstalk.ConnError).Err == beanstalk.ErrTimeout {
			continue
		} else if err.(beanstalk.ConnError).Err == beanstalk.ErrDeadline {
			b.log.Printf("%v (retrying in %v)", err, deadlineSoonDelay)
			time.Sleep(deadlineSoonDelay)
			continue
		} else {
			panic(err)
		}
	}
}

// Run connects to beanstalkd and starts broking.
// If ticks channel is present, one job is processed per tick.
func (b *Broker) Run(ticks chan bool) {
	b.log.Println("command:", b.Cmd)
	b.log.Println("connecting to", b.Address)
	conn, err := beanstalk.Dial("tcp", b.Address)
	if err != nil {
		panic(err)
	}

	b.log.Println("watching", b.Tube)
	ts := beanstalk.NewTubeSet(conn, b.Tube)

	for {
		if ticks != nil {
			if _, ok := <-ticks; !ok {
				break
			}
		}

		b.log.Println("reserve (waiting for job)")
		id, body := b.mustReserveWithoutTimeout(ts)
		job := &job{id: id, body: body, conn: conn}

		t, err := job.timeouts()
		if err != nil {
			log.Panic(err)
		}
		if t > 0 {
			b.log.Printf("job %d has %d timeouts, burying", job.id, t)
			job.bury()
			if b.results != nil {
				b.results <- &JobResult{JobId: job.id, Buried: true}
			}
			continue
		}

		b.log.Printf("executing job %d", job.id)
		result, err := b.executeJob(job, b.Cmd)
		if err != nil {
			log.Panic(err)
		}

		err = b.handleResult(job, result)
		if err != nil {
			log.Panic(err)
		}

		if result.Error != nil {
			b.log.Println("result had error:", result.Error)
		}

		if b.results != nil {
			b.results <- result
		}
	}

	b.log.Println("broker finished")
}

func (b *Broker) executeJob(job *job, shellCmd string) (result *JobResult, err error) {
	result = &JobResult{JobId: job.id, Executed: true}

	ttr, err := job.timeLeft()
	timer := time.NewTimer(ttr + ttrMargin)
	if err != nil {
		return
	}

	cmd, stdout, err := startCommand(shellCmd, job.body)
	if err != nil {
		return
	}

	waitC := make(chan error)
	go func() {
		waitC <- cmd.Wait()
	}()

	stdoutC := make(chan []byte)
	go readToChannel(stdout, stdoutC)

	for {
		select {
		case err = <-waitC:
			timer.Stop()
			if e1, ok := err.(*exec.ExitError); ok {
				result.ExitStatus = e1.Sys().(syscall.WaitStatus).ExitStatus()
				err = nil // not a executeJob error
			}
			return
		case <-timer.C:
			b.log.Printf("Sending SIGTERM to worker PID %d", cmd.Process.Pid)
			cmd.Process.Signal(syscall.SIGTERM)
			result.TimedOut = true
			// TODO: follow up with SIGKILL if still running.
		case data := <-stdoutC:
			b.log.Printf("stdout:\n%s", data)
			result.Stdout = append(result.Stdout, data...)
		}
	}
}

func (b *Broker) handleResult(job *job, result *JobResult) (err error) {
	if result.TimedOut {
		b.log.Printf("job %d timed out", job.id)
		return
	}
	b.log.Printf("job %d finished with exit(%d)", job.id, result.ExitStatus)
	switch result.ExitStatus {
	case 0:
		b.log.Printf("deleting job %d", job.id)
		err = job.delete()
	case 1:
		b.log.Printf("releasing job %d", job.id)
		err = job.release()
	default:
		err = fmt.Errorf("Unhandled exit status %d", result.ExitStatus)
	}
	return
}

func startCommand(shellCmd string, input []byte) (cmd *exec.Cmd, stdout io.ReadCloser, err error) {
	cmd = exec.Command("/bin/bash", "-c", shellCmd)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return
	}

	stdout, err = cmd.StdoutPipe()
	if err != nil {
		return
	}

	cmd.Stderr = os.Stderr

	err = cmd.Start()
	if err != nil {
		return
	}

	_, err = stdin.Write(input)
	if err != nil {
		return
	}
	stdin.Close()

	return
}

func readToChannel(reader io.Reader, c chan []byte) {
	buf := make([]byte, 1024)
	for {
		_, err := reader.Read(buf)
		if err != nil {
			break
		}
	}
	c <- buf
}
