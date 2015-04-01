package metrics

import (
	"crypto/rand"
	"fmt"
	"net/http"
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

func createError(err error) {
}

func TestCreate(t *testing.T) {
	c, err := integrationClient()
	if err != nil {
		t.Error(err.Error())
	}

	md := MetricDefinition{Id: "test.metric.create.numeric.1"}
	if err = c.Create(Numeric, md); err != nil {
		t.Error(err.Error())
	}

	// Try to recreate the same..
	err = c.Create(Numeric, md)

	if err != nil {
		if err, ok := err.(*HawkularClientError); ok {
			if err.Code != http.StatusConflict {
				t.Errorf("Should have received conflict code, instead got %d", err.Code)
			}
		} else {
			t.Errorf("Could not parse error reply from Hawkular, %s", err.Error())
		}
	} else {
		t.Fail()
	}

	// Use tags and dataRetention

	tags := make(map[string]string)
	tags["units"] = "bytes"
	tags["env"] = "unittest"
	md_tags := MetricDefinition{Id: "test.metric.create.numeric.2", Tags: tags}
	if err = c.Create(Numeric, md_tags); err != nil {
		t.Errorf(err.Error())
	}

	md_reten := MetricDefinition{Id: "test.metric.create.availability.1", RetentionTime: 12}
	if err = c.Create(Availability, md_reten); err != nil {
		t.Errorf(err.Error())
	}

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
		t.Errorf("Received %d metrics instead of 1", len(metrics))
	}

	metrics, err = c.QuerySingleNumericMetric("test.numeric.single.2", params)

	if len(metrics) != 1 {
		t.Errorf("Received %d metrics instead of 1", len(metrics))
	} else {
		if metrics[0].Timestamp < 1 {
			t.Error("Timestamp was not correctly populated")
		}
	}

}

func TestAddNumericMulti(t *testing.T) {

	if c, err := integrationClient(); err == nil {

		mone := Metric{Value: 1.45, Timestamp: UnixMilli()}
		hone := MetricHeader{Id: "test.multi.numeric.1",
			Data: []Metric{mone}}

		mtwo_1 := Metric{Value: 2, Timestamp: UnixMilli()}

		mtwo_2_t := UnixMilli() - 1e3

		mtwo_2 := Metric{Value: float64(4.56), Timestamp: mtwo_2_t}
		htwo := MetricHeader{Id: "test.multi.numeric.2", Data: []Metric{mtwo_1, mtwo_2}}

		h := []MetricHeader{hone, htwo}

		err = c.WriteMultiple(Numeric, h)
		if err != nil {
			t.Error(err)
		}

		var getMetric = func(id string) []Metric {
			metric, err := c.QuerySingleNumericMetric(id, make(map[string]string))
			if err != nil {
				t.Error(err)
			}
			return metric
		}

		m := getMetric("test.multi.numeric.1")
		if len(m) != 1 {
			t.Errorf("Received %d metrics instead of 1", len(m))
		}

		m = getMetric("test.multi.numeric.2")
		if len(m) != 2 {
			t.Errorf("Received %d metrics, expected 2", len(m))
		}
	} else {
		t.Error(err)
	}
}

func TestCheckErrors(t *testing.T) {
	c, err := integrationClient()
	if err != nil {
		t.Fail()
	}

	if err = c.PushSingleNumericMetric("test.number.as.string", Metric{Value: "notFloat"}); err == nil {
		t.Fail()
	}

	if _, err = c.QuerySingleNumericMetric("test.not.existing", make(map[string]string)); err != nil {
		t.Error("Not existing should not generate an error")
	}
}
