package dsql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

var cfg = Config{
	BrokerAddr:   "localhost:8082",
	PingEndpoint: "/status/health",
	// User:         "druidUsername",
	// Passwd:       "druidPassword",
}

func startMockServer(handler http.HandlerFunc) (ts *httptest.Server, url string) {
	ts = httptest.NewServer(handler)
	return ts, ts.URL
}

func startMockUnstartedServer(handler http.HandlerFunc) (ts *httptest.Server, url string) {
	ts = httptest.NewUnstartedServer(handler)
	return ts, "http://" + ts.Listener.Addr().String()
}

func constructMockResults(header []interface{}, rows [][]interface{}) (b []byte, err error) {
	mockResults := &bytes.Buffer{}

	h, err := json.Marshal(header)
	if err != nil {
		return mockResults.Bytes(), err
	}

	mockResults.Write(h)
	mockResults.WriteString("\n")

	for _, row := range rows {
		r, err := json.Marshal(row)
		if err != nil {
			return mockResults.Bytes(), err
		}

		mockResults.Write(r)
		mockResults.WriteString("\n")
	}

	fmt.Println(mockResults.String())

	return mockResults.Bytes(), nil
}

func TestConnect(t *testing.T) {
	_, err := sql.Open("druid", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
}

func TestPing(t *testing.T) {
	ts, url := startMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer ts.Close()

	cfg.BrokerAddr = url

	db, err := sql.Open("druid", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPingWithError(t *testing.T) {
	// @todo fix test here
	ts, url := startMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer ts.Close()

	cfg.BrokerAddr = url
	db, err := sql.Open("druid", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}

	err = db.Ping()
	if err != ErrPinging {
		t.Fatal("expected ping error but did not receive")
	}
}

func TestQuery(t *testing.T) {
	header := []interface{}{"__time", "added", "channel"}
	mockRows := [][]interface{}{{"2015-09-12T00:46:58.771Z", 36, "#en.wikipedia"}, {"2015-09-12T00:46:58.772Z", 76, "#ca.wikipedia"}}

	output, _ := constructMockResults(header, mockRows)
	ts, url := startMockServer(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(output)
		w.Header().Add("Content-Type", "application/json")
	})
	defer ts.Close()

	cfg.BrokerAddr = url
	db, err := sql.Open("druid", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT __time, added, channel FROM \"wikiticker-2015-09-12-sampled\" LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}

	var channel string
	var time string
	var added int
	var channels []string
	var times []string
	var addeds []int
	row := 0

	for rows.Next() {
		err := rows.Scan(&time, &added, &channel)
		if err != nil {
			t.Error(err)
		}
		if time != mockRows[row][0] {
			t.Fatalf("Expecting %v got %v", mockRows[row][0], time)
		}
		if added != mockRows[row][1] {
			t.Fatalf("Expecting %v got %v", mockRows[row][1], added)
		}
		if channel != mockRows[row][2] {
			t.Fatalf("Expecting %v got %v", mockRows[row][2], channel)
		}
		channels = append(channels, channel)
		times = append(times, time)
		addeds = append(addeds, added)
		row++
	}
	if len(times) != len(mockRows) || len(channels) != len(mockRows) || len(addeds) != len(mockRows) {
		t.Error("Did not fetch results properly")
	}
}

func TestQueryContextWithCancel(t *testing.T) {
	header := []interface{}{"__time"}
	mockRows := [][]interface{}{{"2015-09-12T00:46:58.771Z"}, {"2015-09-12T00:46:58.772Z"}}
	output, _ := constructMockResults(header, mockRows)
	ts, url := startMockUnstartedServer(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Second * 2)
		_, _ = w.Write(output)
		w.Header().Add("Content-Type", "application/json")
	})
	ts.Start()
	defer ts.Close()

	cfg.BrokerAddr = url

	db, err := sql.Open("druid", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = db.QueryContext(ctx, "SELECT __time FROM \"wikiticker-2015-09-12-sampled\" LIMIT 10")
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatal(err)
	}
}

func TestQueryWithoutCancellation(t *testing.T) {
	header := []interface{}{"__time"}
	mockRows := [][]interface{}{{"2015-09-12T00:46:58.771Z"}, {"2015-09-12T00:46:58.772Z"}}
	output, _ := constructMockResults(header, mockRows)
	ts, url := startMockServer(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(output)
		w.Header().Add("Content-Type", "application/json")
	})
	defer ts.Close()
	cfg.BrokerAddr = url
	db, err := sql.Open("druid", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT __time FROM \"wikiticker-2015-09-12-sampled\" LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}

	var time string
	var times []string
	row := 0

	for rows.Next() {
		err := rows.Scan(&time)
		if err != nil {
			t.Error(err)
		}
		if time != mockRows[row][0] {
			t.Fatalf("Expecting %v got %v", mockRows[row][0], time)
		}
		times = append(times, time)
		row++
	}

	if len(times) != len(mockRows) {
		t.Error("Did not fetch results properly")
	}
}
