package client

// Hawkular-Metrics external structs

type MetricHeader struct {
	Id   string    `json:"id"`
	Data []*Metric `json:"data"`
}

// Value should be convertible to float64 for numeric values
// Timestamp is milliseconds since epoch
type Metric struct {
	Timestamp int64       `json:"timestamp"`
	Value     interface{} `json:"value"`
}

type HawkularError struct {
	ErrorMsg string `json:"errorMsg"`
}
