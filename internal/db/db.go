// Package db wires up the bun database handle used across the app.
package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/uptrace/bun/extra/bundebug"
)

// Open opens a Postgres connection using go-bun's pgdriver and verifies it with
// a ping. dsn is a standard Postgres URL, e.g.
// postgres://user:pass@localhost:5432/dbname?sslmode=disable
// When verbose is true, every query is logged via bundebug.
func Open(ctx context.Context, dsn string, verbose bool) (*bun.DB, error) {
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))

	db := bun.NewDB(sqldb, pgdialect.New())
	if verbose {
		db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(true)))
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
