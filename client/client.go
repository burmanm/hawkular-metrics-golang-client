package client

// package metrics instead? As this is metrics-only client, not other Hawkular..

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strconv"
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

// Sent and received stuff

type MetricHeader struct {
	Id   string    `json:"id"`
	Data []*Metric `json:"data"`
}

// Value is mandatory, Timestamp is optional. Value should be convertible to float64 for numeric values
// Timestamp is milliseconds since epoch
type Metric struct {
	Timestamp int64       `json:"timestamp"`
	Value     interface{} `json:"value"`
}

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

// Take input of single Metric instance and modify it to fit our multiPut
func (self *Client) PushSingleNumericMetric(id string, m Metric) error {
	f, err := self.convertToFloat64(m.Value)
	if err != nil {
		return err
	}

	nM := &Metric{Timestamp: m.Timestamp, Value: f}
	if nM.Timestamp == 0 {
		nM.Timestamp = time.Now().UnixNano() / 1e6
	}

	mH := MetricHeader{Id: id, Data: []*Metric{nM}}
	return self.WriteMultiple(self.metricType(nM.Value), []MetricHeader{mH})
}

func (self *Client) QuerySingleNumericMetric(id string, options map[string]string) ([]Metric, error) {
	g, err := self.paramUrl(self.dataUrl(self.singleMetricsUrl("numeric", id)), options)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(g)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return []Metric{}, nil
	} else if resp.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		metrics := []Metric{}
		err = json.Unmarshal(b, &metrics)
		if err != nil {
			return nil, err
		}
		return metrics, nil
	} else {
		return nil, self.parseErrorResponse(resp)
	}
}

func (self *Client) WriteMultiple(metricType string, metrics []MetricHeader) error {
	json, err := json.Marshal(&metrics)
	if err != nil {
		return err
	}
	return self.write(self.dataUrl(self.metricsUrl(metricType)), json)
}

func (self *Client) write(url string, json []byte) error {
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(json))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
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

// func (self *Client) metricsDataUrl(metricType string) string {
// 	return fmt.Sprintf("%s/data", self.metricsUrl(metricType))
// }

func (self *Client) singleMetricsUrl(metricType string, id string) string {
	return fmt.Sprintf("%s/%s", self.metricsUrl(metricType), id)
}

func (self *Client) dataUrl(url string) string {
	return fmt.Sprintf("%s/data", url)
}

func (self *Client) paramUrl(starturl string, options map[string]string) (string, error) {
	u, err := url.Parse(starturl)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for k, v := range options {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (self *Client) metricType(value interface{}) string {
	var mType string
	if _, ok := value.(float64); ok {
		mType = "numeric"
	} else {
		mType = "availability"
	}
	return mType
}

func (self *Client) convertToFloat64(v interface{}) (float64, error) {
	switch i := v.(type) {
	case float64:
		return float64(i), nil
	case float32:
		return float64(i), nil
	case int64:
		return float64(i), nil
	case int32:
		return float64(i), nil
	case int16:
		return float64(i), nil
	case int8:
		return float64(i), nil
	case uint64:
		return float64(i), nil
	case uint32:
		return float64(i), nil
	case uint16:
		return float64(i), nil
	case uint8:
		return float64(i), nil
	case int:
		return float64(i), nil
	case uint:
		return float64(i), nil
	case string:
		f, err := strconv.ParseFloat(i, 64)
		if err != nil {
			return math.NaN(), err
		}
		return f, err
	default:
		return math.NaN(), fmt.Errorf("Cannot convert %s to float64", i)
	}
}
