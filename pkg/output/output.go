package output

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/olivere/elastic.v6"
	"k8s.io/klog"
	"net/http"
	"syscall"
	"time"
)

type ElasticsearchRetrier struct {
	backoff elastic.Backoff
	retry   int
}

type Parameter struct {
	Addr        []string
	BulkLimit   int
	Retry       int
	IndexPrefix string
}

type Output struct {
	isClose bool
	*elastic.Client
	Bulk      *elastic.BulkService
	Parameter Parameter
}

func NewElasticsearchRetrier(retry int) *ElasticsearchRetrier {
	return &ElasticsearchRetrier{
		backoff: elastic.NewExponentialBackoff(10*time.Millisecond, 8*time.Second),
		retry:   retry,
	}
}

func (r *ElasticsearchRetrier) Retry(ctx context.Context, retry int, req *http.Request, resp *http.Response, err error) (time.Duration, bool, error) {

	// Fail hard on a specific error
	if err == syscall.ECONNREFUSED {
		return 0, false, errors.New("elasticsearch or network down")
	}

	// Stop after 5 retries
	if retry >= r.retry {
		klog.Errorf("elasticsearch retry bulk reached limit")
		return 0, false, nil
	}

	// Let the backoff strategy decide how long to wait and whether to stop
	wait, stop := r.backoff.Next(retry)
	return wait, stop, nil
}

func (c *Output) GenerateTimeIndex() string {
	return fmt.Sprintf("%s-%s", c.Parameter.IndexPrefix, time.Now().Format("2006.01.02"))
}

func (c *Output) Send() error {

	// 生成开始时间戳
	start := time.Now()

	// 开始向ES插入数据(数据取决于配置文件定义的数量)
	if handler, err := c.Bulk.Do(context.Background()); err != nil {
		d, _ := json.Marshal(handler.Failed())
		return errors.New(string(d))
	}

	// 生成写入时间
	elapsed := time.Since(start)

	klog.Infof("bulk elapsed: %s", elapsed)
	return nil
}

func (c *Output) BufferSize() int {
	return c.Bulk.NumberOfActions()
}

func (c *Output) BufferLimit() int {
	return c.Parameter.BulkLimit
}

func (c *Output) AddBuffer(data interface{}) {

	// 生成索引名称
	index := c.GenerateTimeIndex()

	// 生成ES数据
	req := elastic.NewBulkIndexRequest().Index(index).Type("kafka-forwarder-elasticsearch").Doc(data)

	// 追加到缓存中
	c.Bulk = c.Bulk.Add(req)
}

func (c *Output) Close() {

	if c.BufferSize() > 0 {
		klog.Warningf("[ output ] The data in the remaining memory is written to es: %d", c.BufferSize())
		c.Send()
	}
	c.Client.Stop()

	c.isClose = true
}

func (c *Output) IsClose() bool {
	return c.isClose
}

func NewOutput(parameter Parameter) (*Output, error) {
	var client *elastic.Client
	var err error

	if client, err = elastic.NewClient(
		elastic.SetURL(parameter.Addr...),
		elastic.SetRetrier(NewElasticsearchRetrier(parameter.Retry)),
	); err != nil {
		return nil, err
	}

	return &Output{
		Parameter: parameter,
		Client:    client,
		Bulk:      client.Bulk(),
	}, nil
}
