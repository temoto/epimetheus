package main

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/coreos/go-systemd/daemon"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type Metrics struct {
	Filled          bool
	ListenOverflows int64
	ListenDrops     int64
	Rest            map[string]int64
}

func (self Metrics) Sub(m2 Metrics) (result Metrics) {
	result = self
	result.ListenOverflows -= m2.ListenOverflows
	result.ListenDrops -= m2.ListenDrops
	return
}

var (
	awsConfig         *aws.Config
	awsSession        *session.Session
	cloudwatchService *cloudwatch.CloudWatch
	hostname          string
	current           Metrics
	previous          Metrics
)

func parseNetstat(s []byte) Metrics {
	newline := []byte("\n")
	m := make(map[string]int64)

	lines := bytes.Split(s, newline)
	if len(lines) >= 2 {
		headers := bytes.Fields(lines[0])
		values := bytes.Fields(lines[1])
		//assert len(headers)==len(values)
		for i := 1; i < len(headers) && i < len(values); i++ {
			x, _ := strconv.ParseInt(string(values[i]), 10, 64)
			m[string(headers[i])] = x
		}
	}

	met := Metrics{
		Filled:          true,
		ListenOverflows: m["ListenOverflows"],
		ListenDrops:     m["ListenDrops"],
		Rest:            m,
	}
	return met
}

func poll() error {
	data, err := ioutil.ReadFile("/proc/net/netstat")
	if err != nil {
		log.Print("netstat read error:", err)
		return err
	}
	m := parseNetstat(data)
	if !previous.Filled {
		current = Metrics{Filled: true}
	} else {
		current = m.Sub(previous)
	}
	previous = m
	return nil
}

func send(ctx context.Context, now time.Time) error {
	var err error
	dims := []*cloudwatch.Dimension{
		&cloudwatch.Dimension{Name: aws.String("hostname"), Value: aws.String(hostname)},
	}
	_, err = cloudwatchService.PutMetricDataWithContext(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String("machine"),
		MetricData: []*cloudwatch.MetricDatum{
			&cloudwatch.MetricDatum{
				MetricName: aws.String("Netstat-Listen-Overflows"),
				Dimensions: dims,
				Timestamp:  &now,
				Value:      aws.Float64(float64(current.ListenOverflows)),
			},
			&cloudwatch.MetricDatum{
				MetricName: aws.String("Netstat-Listen-Drops"),
				Dimensions: dims,
				Timestamp:  &now,
				Value:      aws.Float64(float64(current.ListenDrops)),
			},
		},
	})
	if err != nil {
		log.Print(err)
	}
	log.Printf("send success listen-overflows=%d listen-drops=%d", current.ListenOverflows, current.ListenDrops)
	return err
}

func main() {
	var err error
	log.Print("Starting Epimetheus...")

	// systemd service
	sdnotify("READY=0\nSTATUS=init\n")
	if wdTime, err := daemon.SdWatchdogEnabled(true); err != nil {
		log.Fatal(err)
	} else if wdTime != 0 {
		go func() {
			for range time.Tick(wdTime) {
				sdnotify("WATCHDOG=1\n")
			}
		}()
	}

	awsSession, err = session.NewSession()
	if err != nil {
		panic(err)
	}
	awsConfig = aws.NewConfig().WithLogger(aws.LoggerFunc(func(args ...interface{}) {
		log.Print(args...)
	})).WithLogLevel(aws.LogOff)
	cloudwatchService = cloudwatch.New(awsSession, awsConfig)

	if hostname, err = os.Hostname(); err != nil {
		panic(err)
	}

	ctx := context.Background()
	sigShutdownChan := make(chan os.Signal, 1)
	signal.Notify(sigShutdownChan, syscall.SIGINT, syscall.SIGTERM)
	sdnotify("READY=1\nSTATUS=work\n")
	log.Print("init ok")
	poll()
	send(ctx, time.Now())
	ticker := time.NewTicker(53 * time.Second)
mainLoop:
	for {
		select {
		case now := <-ticker.C:
			poll()
			send(ctx, now)
		case <-sigShutdownChan:
			log.Printf("graceful stop")
			sdnotify("READY=0\nSTATUS=stopping\n")
			break mainLoop
		}
	}
}

func sdnotify(s string) {
	if _, err := daemon.SdNotify(false, s); err != nil {
		log.Fatal(err)
	}
}
