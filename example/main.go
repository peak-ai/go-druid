package main

import (
	"log"

	"database/sql"

	"github.com/peak-ai/go-druid/dsql"
)

// Entry -
type Entry struct {
	Channel string `json:"channel" db:"channel"`
	Comment string `json:"comment" db:"comment"`
}

// avatica is a project for making Druid compatible with the Go standard library SQL packages (and packages compliant with the standard library package)
func newConnection() (*sql.DB, error) {
	cfg := dsql.Config{
		BrokerAddr:   "localhost:8082",
		PingEndpoint: "/status/health",
	}
	db, err := sql.Open("druid", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}

	return db, nil
}

func main() {
	conn, err := newConnection()
	if err != nil {
		log.Panic(err)
	}

	if err := conn.Ping(); err != nil {
		log.Fatal("Cannot ping da ting")
	}

	rows, err := conn.Query("SELECT * FROM wikipedia")
	if err != nil {
		log.Panic(err)
	}

	log.Println(rows)
}
