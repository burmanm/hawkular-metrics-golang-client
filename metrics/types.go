package metrics

// Hawkular-Metrics external structs

type MetricHeader struct {
	Id   string   `json:"id"`
	Data []Metric `json:"data"`
}

// Value should be convertible to float64 for numeric values
// Timestamp is milliseconds since epoch
// Tags here?
type Metric struct {
	Timestamp int64       `json:"timestamp"`
	Value     interface{} `json:"value"`
}

type HawkularError struct {
	ErrorMsg string `json:"errorMsg"`
}

type MetricDefinition struct {
	Id            string            `json:"id"`
	Tags          map[string]string `json:"tags,omitempty"`
	RetentionTime int               `json:"dataRetention,omitempty"`
}
