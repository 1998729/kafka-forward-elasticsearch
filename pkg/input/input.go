package input

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"k8s.io/klog"
)

type Parameter struct {
	Addr                 []string
	Group                string
	Topic                string
	ConsumerReturnErrors bool
	OffsetsInitial       int64

	Partitions chan cluster.PartitionConsumer
}

type Input struct {
	client     *cluster.Consumer
	Partitions chan cluster.PartitionConsumer
}

func (c *Input) Start() error {

	go func() {
		for err := range c.client.Errors() {
			klog.Errorf("Error: %s\n", err.Error())
		}
	}()

	klog.Infof("[ input ] worker is start")

	for {
		select {
		case part, ok := <-c.client.Partitions():
			if !ok {
				return errors.New(fmt.Sprintf("input-kafka-partition-worker is stopped"))
			}
			klog.Info("[ input ] send partition worker signal")
			c.Partitions <- part
		case _, ok := <-c.Partitions:
			if !ok {
				return nil
			}
		}
	}
}

func (c *Input) Stop() error {
	return c.client.Close()
}

func NewInput(parameter Parameter) (*Input, error) {
	var client *cluster.Consumer
	var err error

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = parameter.ConsumerReturnErrors
	config.Consumer.Offsets.Initial = parameter.OffsetsInitial
	config.Group.Mode = cluster.ConsumerModePartitions
	config.Version = sarama.V0_10_2_0

	if client, err = cluster.NewConsumer(
		parameter.Addr,
		parameter.Group,
		[]string{parameter.Topic},
		config,
	); err != nil {
		return nil, err
	}

	return &Input{
		client:     client,
		Partitions: parameter.Partitions,
	}, nil
}
