package forward

import (
	cluster "github.com/bsm/sarama-cluster"
	"k8s.io/klog"
	"kafka-forward-elasticsearch/pkg/filter"
	"kafka-forward-elasticsearch/pkg/input"
	"kafka-forward-elasticsearch/pkg/output"
	"sync"
)

type Forwarder struct {
	FilterClaim filter.IClaimFunc

	input *input.Input

	outputParameter output.Parameter

	partitionChan chan cluster.PartitionConsumer

	workers []*output.Output

	wait *sync.WaitGroup
}

func Consume(part cluster.PartitionConsumer, output *output.Output, filterClaim filter.IClaimFunc, wait *sync.WaitGroup) {

	for msg := range part.Messages() {

		if message, err := filterClaim(part.Topic(), msg.Value); err == nil {
			output.AddBuffer(message)

			if output.BufferSize() == output.BufferLimit() {
				if err = output.Send(); err != nil {
					klog.Errorf("failed to write data to elasticsearch-worker-%d -> %s", part.Partition(), err.Error())
				}
			}
		}
		part.MarkOffset(msg.Offset, "")
	}

	output.Close()
	wait.Done()
	klog.Infof("[ forwarder-consume-%d] is stopped", part.Partition())
}

func (c *Forwarder) Start() {

	go c.input.Start()

	for p := range c.partitionChan {

		outputClient, err := output.NewOutput(c.outputParameter)

		if err != nil {
			klog.Errorf("init output error: %s", err.Error())
			continue
		}

		c.workers = append(c.workers, outputClient)

		klog.Info("[ forwarder ] start consume")

		c.wait.Add(1)
		go Consume(p, outputClient, c.FilterClaim, c.wait)
	}
}

func (c *Forwarder) Stop() {

	if err := c.input.Stop(); err != nil {
		klog.Errorf("input stop error: %s", err.Error())
	}

	close(c.partitionChan)

	c.wait.Wait()
	klog.Info("[ forwarder ] is stopped")
}

func NewForwarder(i input.Parameter, o output.Parameter, f filter.IClaimFunc) (*Forwarder, error) {

	i.Partitions = make(chan cluster.PartitionConsumer)

	inputClient, err := input.NewInput(i)

	if err != nil {
		return nil, err
	}

	return &Forwarder{
		input:           inputClient,
		partitionChan:   i.Partitions,
		outputParameter: o,
		FilterClaim:     f,
		wait:            &sync.WaitGroup{},
	}, nil
}
