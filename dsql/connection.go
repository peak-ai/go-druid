package dsql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"sync"

	"github.com/zencoder/go-smile/smile"
)

var (
	// ErrPinging is an error returned when health check endpoint returns a non 200.
	ErrPinging = errors.New("druid: error fetching health info from druid")

	// ErrCancelled is an error returned when we receive a cancellation event from a context object
	ErrCancelled = errors.New("druid: cancellation received")

	// ErrRequestForm is an error returned when failing to form a request
	ErrRequestForm = errors.New("druid: error forming request")

	// ErrCreatingRequest is an error when creating a new request
	ErrCreatingRequest = errors.New("druid: error creating new request")

	// ErrMakingRequest is an error whilst making a request to the druid server itself
	ErrMakingRequest = errors.New("druid: error making request to druid server")
)

func wrapErr(a, b error) error {
	return fmt.Errorf("%v: %v", a, b)
}

type key int

const (
	transportKey key = iota
	requestKey
)

type connection struct {
	Client    *http.Client
	Cfg       *Config
	closeCh   chan struct{}
	watcherCh chan context.Context
	errorCh   chan error
	resultsCh chan []byte
	requestCh chan *http.Request
	closed    bool
	mtx       sync.Mutex
}

type queryRequest struct {
	Query        string `json:"query"`
	ResultFormat string `json:"resultFormat"`
	Header       bool   `json:"header"`
}

type queryResponse [][]interface{}

// Prepare implements db.Conn.Prepare and returns a noop statement
func (c *connection) Prepare(stmt string) (driver.Stmt, error) {
	return &statementNoop{}, driver.ErrSkip
}

// Close closes a connection.
func (c *connection) Close() (err error) {
	if c.closed {
		return
	}
	c.mtx.Lock()
	c.closed = true
	c.mtx.Unlock()
	close(c.closeCh)
	c.Client = nil
	return
}

// Begin implements db.Conn.Prepare and is a noop
func (c *connection) Begin() (tx driver.Tx, err error) {
	tx = &transactionNoop{}
	return tx, driver.ErrSkip
}

// Ping implmements db.conn.Prepare and hits the health endpoint of a broker
func (c *connection) Ping(ctx context.Context) error {
	res, err := c.Client.Get(fmt.Sprintf("%s%s", c.Cfg.BrokerAddr, c.Cfg.PingEndpoint))
	if err != nil {
		return wrapErr(ErrPinging, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return wrapErr(ErrPinging, err)
	}

	return nil
}

func (c *connection) startRequestPipeline() {
	go func() {
		for {
			select {
			case req := <-c.requestCh:
				res, err := c.Client.Do(req)
				if err != nil {
					c.errorCh <- err
					return
				}

				body, err := ioutil.ReadAll(res.Body)
				if err != nil {
					c.errorCh <- err
				}

				// @todo do we still want to do this if there was an error above?
				c.resultsCh <- body
			case <-c.closeCh:
				return
			}
		}
	}()
}

// Query queries the druid sql api
func (c *connection) Query(q string, args []driver.Value) (driver.Rows, error) {
	return c.query(q, args)
}

func (c *connection) makeRequest(q string) (*http.Request, error) {
	queryURL := fmt.Sprintf("%s%s", c.Cfg.BrokerAddr, c.Cfg.QueryEndpoint)
	request := &queryRequest{
		Query:        q,
		ResultFormat: "array",
		Header:       true,
	}

	payload, err := json.Marshal(request)
	if err != nil {
		return nil, wrapErr(ErrRequestForm, err)
	}

	fmt.Printf("GO-DRUID: %+v\n", c.Cfg)
	fmt.Printf("GO-DRUID: %s, %s\n", queryURL, payload)

	req, err := http.NewRequest(http.MethodPost, queryURL, bytes.NewReader(payload))
	if err != nil {
		return nil, wrapErr(ErrRequestForm, err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Selects whether or not to request as JSON, or Jackson Smile encoding
	// https://druid.apache.org/docs/latest/querying/querying.html
	// This might not work with SQL queries...
	if maybeEnv("DRUID_SMILE", "false") == "true" {
		req.Header.Set("Accept", "application/x-jackson-smile")
	}

	return req, nil
}

func (c *connection) parseJSONResponse(body []byte) (queryResponse, error) {
	var results queryResponse
	err := json.Unmarshal(body, &results)
	return results, err
}

func (c *connection) parseSmileResponse(body []byte) (queryResponse, error) {
	decoded, err := smile.DecodeToObject(body)
	if err != nil {
		return queryResponse{}, err
	}

	return decoded.(queryResponse), nil
}

func maybeEnv(a, b string) string {
	val := os.Getenv(a)
	if val == "" {
		return b
	}
	return val
}

func (c *connection) parseResponse(body []byte) (r *rows, err error) {
	var results queryResponse

	if maybeEnv("DRUID_SMILE", "false") == "true" {
		results, err = c.parseSmileResponse(body)
	} else {
		results, err = c.parseJSONResponse(body)
	}

	if err != nil {
		return &rows{}, err
	}

	// No results returned
	if len(results) == 0 {
		return &rows{}, sql.ErrNoRows
	}

	var columnNames []string
	for _, val := range results[0] {
		columnNames = append(columnNames, val.(string))
	}

	var returnedRows [][]field
	for i := 1; i < len(results); i++ {
		var cols []field
		for _, val := range results[i] {
			cols = append(cols, field{Value: reflect.ValueOf(val), Type: reflect.TypeOf(val)})
		}
		returnedRows = append(returnedRows, cols)
	}

	resultSet := resultSet{
		columnNames: columnNames,
		rows:        returnedRows,
		currentRow:  0,
		dateField:   c.Cfg.DateField,
		dateFormat:  c.Cfg.DateFormat,
	}

	r = &rows{
		conn:      c,
		resultSet: resultSet,
	}

	return r, nil
}

func (c *connection) query(q string, args []driver.Value) (*rows, error) {
	req, err := c.makeRequest(q)
	if err != nil {
		return &rows{}, err
	}

	res, err := c.Client.Do(req)
	if err != nil {
		return &rows{}, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return &rows{}, err
	}

	code := res.StatusCode
	if code != http.StatusOK {
		log.Println(string(body))
		return &rows{}, fmt.Errorf("error making query request to druid, status code: %d", code)
	}

	return c.parseResponse(body)
}

func (c *connection) queryContext(ctx context.Context, q string, args []driver.NamedValue) (*rows, error) {
	req, err := c.makeRequest(q)
	if err != nil {
		return &rows{}, wrapErr(ErrCreatingRequest, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	req = req.WithContext(ctx)

	c.requestCh <- req
	tr := &http.Transport{}
	c.Client.Transport = tr

	var r *rows

	select {
	case body := <-c.resultsCh:
		r, err = c.parseResponse(body)
		if err != nil {
			return r, err
		}
	case err = <-c.errorCh:
	case <-ctx.Done():
		err = ctx.Err()
		return r, err
	}

	return r, err
}

// QueryContext -
func (c *connection) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	vals, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}

	if ctx.Done() == nil {
		return c.query(q, vals)
	}

	return c.queryContext(ctx, q, args)
}
