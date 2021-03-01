package main

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/peak-ai/go-druid/dsql"
)

func newConnection() *sqlx.DB {
	cfg := dsql.Config{
		BrokerAddr:   "localhost:8082",
		PingEndpoint: "/status/health",
	}
	db, err := sqlx.Open("druid", cfg.FormatDSN())
	if err != nil {
		log.Panic(err)
	}

	return db
}

func runQuery(conn *sqlx.DB, limit int) {
	query := fmt.Sprintf("select comment from wikipedia limit %d", limit)
	_, err := conn.Queryx(query)
	if err != nil {
		log.Panic(err)
	}
}

func benchmarkScenario(limit int, smile string) {
	fn := func(b *testing.B) {
		os.Setenv("DRUID_SMILE", smile)
		conn := newConnection()
		for n := 0; n < b.N; n++ {
			runQuery(conn, limit)
		}
	}
	r := testing.Benchmark(fn)

	fmt.Printf("Rows: %d - Smile: %s\n", limit, smile)
	fmt.Printf("%d ns/op\n", int(r.T)/r.N)
	fmt.Printf("%d ns/op/i\n", int(r.T)/r.N/limit)
	fmt.Printf("%d ms\n", r.T.Milliseconds())
	fmt.Printf("%d mem\n", r.MemBytes)
}

func main() {
	benchmarkScenario(1, "true")
	benchmarkScenario(1, "false")

	benchmarkScenario(10, "true")
	benchmarkScenario(10, "false")

	benchmarkScenario(100, "true")
	benchmarkScenario(100, "false")

	benchmarkScenario(1000, "true")
	benchmarkScenario(1000, "false")
}
