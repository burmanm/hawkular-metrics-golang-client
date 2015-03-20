package client

// package metrics instead? As this is metrics-only client, not other Hawkular..

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

// Client creation

type Parameters struct {
	Tenant string
	Port   uint16
	Host   string
}

type Client struct {
	tenant  string
	baseurl string
}

// [
//   {
//     "data": [
//       {
//         "timestamp": 1426763542757,
//         "value": 1.45
//       },
//       {
//         "timestamp": 1426763540757,
//         "value": 2.0
//       }
//     ],
//     "name": "test.numeric.multi"
//   }
// ]

// Sent and received stuff

type MetricHeader struct {
	Name string    `json:"name"`
	Data []*Metric `json:"data"`
}

// Name and value are mandatory, Timestamp is optional. Value should be float64 for numeric values
type Metric struct {
	Name      string `json:"-"` // Should we always just fill this from different response?
	Timestamp int64  `json:"timestamp"`
	// Value     float64 `json:"value"`
	Value interface{} `json:"value"`
}

// This is generic error instance?

type HawkularError struct {
	ErrorMsg string `json:"errorMsg"`
}

func NewHawkularClient(p Parameters) (*Client, error) {
	url := fmt.Sprintf("http://%s:%d/hawkular-metrics/", p.Host, p.Port)
	return &Client{
		baseurl: url,
		tenant:  p.Tenant,
	}, nil
}

// func (self *Client) CreateMetric(name string, value float64)

func (self *Client) PushSingleNumericMetric(m *Metric) error {
	if &m.Timestamp == nil {
		m.Timestamp = time.Now().Unix()
	}
	var mType string
	if _, ok := m.Value.(float64); ok {
		mType = "numeric"
	}
	mH := MetricHeader{Name: m.Name, Data: []*Metric{m}}
	return self.WriteMultiple(mType, []MetricHeader{mH})
}

func (self *Client) WriteMultiple(metricType string, metrics []MetricHeader) error {
	json, err := json.Marshal(&metrics)
	if err != nil {
		return err
	}
	return self.write(self.metricsDataUrl(metricType), json)
}

func (self *Client) write(url string, json []byte) error {
	fmt.Println(string(json))
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(json))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// @TODO Detect when 204 is acceptable answer
	if resp.StatusCode != 200 {
		return self.parseErrorResponse(resp)
	}
	return nil
}

func (self *Client) parseErrorResponse(resp *http.Response) error {
	// Parse error messages here correctly..
	reply, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Got status code %d, reply could not be parsed: %s", resp.StatusCode, err.Error())
	}

	details := &HawkularError{}

	err = json.Unmarshal(reply, details)
	if err != nil {
		return fmt.Errorf("Got status code %d, reply could not be parsed: %s", resp.StatusCode, err.Error())
	}

	return fmt.Errorf("Got status code %d, error: %s", resp.StatusCode, details.ErrorMsg)
}

func (self *Client) metricsUrl(metricType string) string {
	return fmt.Sprintf("%s%s/metrics/%s", self.baseurl, self.tenant, metricType)
}

func (self *Client) metricsDataUrl(metricType string) string {
	return fmt.Sprintf("%s/data", self.metricsUrl(metricType))
}
