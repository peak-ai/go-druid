package main

import (
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/peak-ai/go-druid/dsql"
)

// Entry -
type Entry struct {
	Channel string `json:"channel" db:"channel"`
	Comment string `json:"comment" db:"comment"`
}

// avatica is a project for making Druid compatible with the Go standard library SQL packages (and packages compliant with the standard library package)
func newConnection() (*sqlx.DB, error) {
	cfg := dsql.Config{
		BrokerAddr:   "localhost:8082",
		PingEndpoint: "/status/health",
		DateFormat:   "iso",
		DateField:    "created_at",
	}
	db, err := sqlx.Open("druid", cfg.FormatDSN())
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
		log.Fatal(err)
	}

	rows, err := conn.Queryx("select comment from wikipedia limit 10")
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var result Entry
		if err := rows.StructScan(&result); err != nil {
			log.Panic(err)
		}

		log.Printf("%v\n", result)
	}
}
