package main

import (
	"k8s.io/klog"
	forward "kafka-forward-elasticsearch/pkg/forwarde"
	"kafka-forward-elasticsearch/pkg/input"
	"kafka-forward-elasticsearch/pkg/output"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	doneCh := make(chan os.Signal)

	// input required parameter
	inputParameter := input.Parameter{
		// kafka connect address
		Addr: []string{
			"192.168.1.1:9092",
			"192.168.1.2:9092",
		},
		// kafka consume group
		Group: "kafka-group",
		// kafka consume topic
		Topic: "kafka-topic",
		// output consume return errors message
		ConsumerReturnErrors: true,
		// consume mode
		// - -1 news consume
		// - 0 old consume
		OffsetsInitial: -1,
	}

	// output required parameter
	outputParameter := output.Parameter{
		// es connect address
		Addr: []string{
			"http://192.168.1.1:9270",
			"http://192.168.1.2:9270",
		},
		// es batch insert count
		BulkLimit: 10000,
		// es insert retry count
		Retry: 10,
		// es index prefix
		// format: es-index-prefix-2022.09.16
		IndexPrefix: "es-index-prefix",
	}

	// custom message design
	// add extra filed
	filterClaimFunc := func(topic string, msg []byte) ([]byte, error) {
		klog.Infof("topic: %s, msg: %s", topic, msg)
		return msg, nil
	}

	forwarder, err := forward.NewForwarder(
		inputParameter,
		outputParameter,
		filterClaimFunc,
	)

	if err != nil {
		klog.Errorf("forwarder init fail: %s", err.Error())
		return
	}

	go forwarder.Start()

	// listen signal
	signal.Notify(doneCh,
		syscall.SIGTERM,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGQUIT,
		os.Interrupt,
		os.Kill,
	)

	<-doneCh

	forwarder.Stop()
}
