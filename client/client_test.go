package client

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"
)

func integrationClient() (*Client, error) {
	t, err := randomTenant()
	if err != nil {
		return nil, err
	}
	p := Parameters{Tenant: t, Port: 8081, Host: "localhost"}
	return NewHawkularClient(p)
}

func randomTenant() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%X", b[:]), nil
}

func TestAddNumericSingle(t *testing.T) {
	c, err := integrationClient()
	if err != nil {
		t.Error(err.Error())
	}

	// With timestamp
	m := Metric{Timestamp: time.Now().UnixNano() / 1e6, Value: 1.34}
	if err = c.PushSingleNumericMetric("test.numeric.single.1", m); err != nil {
		t.Error(err.Error())
	}

	// Without preset timestamp
	m = Metric{Value: 2}
	if err = c.PushSingleNumericMetric("test.numeric.single.2", m); err != nil {
		t.Error(err.Error())
	}

	// Query for both metrics and check that they're correctly filled
	params := make(map[string]string)
	metrics, err := c.QuerySingleNumericMetric("test.numeric.single.1", params)
	if err != nil {
		t.Error(err)
	}

	if len(metrics) != 1 {
		t.Error(fmt.Errorf("Received %d metrics instead of 1", len(metrics)))
	}

	metrics, err = c.QuerySingleNumericMetric("test.numeric.single.2", params)

	if len(metrics) != 1 {
		t.Error(fmt.Errorf("Received %d metrics instead of 1", len(metrics)))
	} else {
		if metrics[0].Timestamp < 1 {
			t.Error(fmt.Errorf("Timestamp was not correctly populated"))
		}
	}

}

func TestAddNumericMulti(t *testing.T) {
}
