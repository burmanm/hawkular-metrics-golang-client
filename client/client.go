package client

// package metrics instead? As this is metrics-only client, not other Hawkular..

// TODO: CreateMetrics interfaces, support tags, remove *Metric (we don't want pointers)

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

// MetricType restrictions
type MetricType int

const (
	Numeric = iota
	Availability
)

var longForm = []string{
	"numeric",
	"availability",
}

var shortForm = []string{
	"num",
	"avail",
}

func (self MetricType) validate() error {
	if int(self) > len(longForm) && int(self) > len(shortForm) {
		return fmt.Errorf("Given MetricType value %d is not valid", self)
	}
	return nil
}

func (self MetricType) String() string {
	if err := self.validate(); err != nil {
		return "unknown"
	}
	return longForm[self]
}

func (self MetricType) shortForm() string {
	if err := self.validate(); err != nil {
		return "unknown"
	}
	return shortForm[self]
}

// Client creation and instance config

type Parameters struct {
	Tenant string
	Port   uint16
	Host   string
}

type Client struct {
	Tenant  string
	Baseurl string
}

func NewHawkularClient(p Parameters) (*Client, error) {
	url := fmt.Sprintf("http://%s:%d/hawkular-metrics/", p.Host, p.Port)
	return &Client{
		Baseurl: url,
		Tenant:  p.Tenant,
	}, nil
}

// Take input of single Metric instance. If Timestamp is not defined, use current time
func (self *Client) PushSingleNumericMetric(id string, m Metric) error {
	f, err := ConvertToFloat64(m.Value)
	if err != nil {
		return err
	}

	nM := &Metric{Timestamp: m.Timestamp, Value: f}
	if nM.Timestamp == 0 {
		nM.Timestamp = UnixMilli()
	}

	mH := MetricHeader{Id: id, Data: []*Metric{nM}}
	return self.WriteMultiple(self.metricType(nM.Value), []MetricHeader{mH})
}

func (self *Client) QuerySingleNumericMetric(id string, options map[string]string) ([]Metric, error) {
	return self.query(self.dataUrl(self.singleMetricsUrl(Numeric, id)), options)
}

func (self *Client) WriteMultiple(metricType MetricType, metrics []MetricHeader) error {
	if err := metricType.validate(); err != nil {
		return err
	}

	json, err := json.Marshal(&metrics)
	if err != nil {
		return err
	}
	return self.write(self.dataUrl(self.metricsUrl(metricType)), json)
}

func (self *Client) query(url string, options map[string]string) ([]Metric, error) {
	g, err := self.paramUrl(url, options)
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

func (self *Client) write(url string, json []byte) error {
	if resp, err := http.Post(url, "application/json", bytes.NewBuffer(json)); err == nil {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return self.parseErrorResponse(resp)
		}
		return nil
	} else {
		return err
	}
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

func (self *Client) metricsUrl(metricType MetricType) string {
	return fmt.Sprintf("%s%s/metrics/%s", self.Baseurl, self.Tenant, metricType.String())
}

func (self *Client) singleMetricsUrl(metricType MetricType, id string) string {
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

func (self *Client) metricType(value interface{}) MetricType {
	if _, ok := value.(float64); ok {
		return Numeric
	} else {
		return Availability
	}
}

// Following methods are to ease the work of the client users

func ConvertToFloat64(v interface{}) (float64, error) {
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

// Returns
func UnixMilli() int64 {
	return time.Now().UnixNano() / 1e6
}
