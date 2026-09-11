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
//
// maxOpenConns bounds the pool so dd cannot exhaust the server's connection slots
// under load (its per-request block-store writes would otherwise open one
// connection per concurrent query — SQLSTATE 53300, "too many clients already",
// especially when the DB is shared). Queries queue on a full pool instead. A value
// <1 leaves the pool unbounded (the database/sql default). The LISTEN/NOTIFY
// watcher uses its own dedicated connection outside this pool, so bounding it never
// starves change notifications.
func Open(ctx context.Context, dsn string, verbose bool, maxOpenConns int) (*bun.DB, error) {
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))

	if maxOpenConns > 0 {
		sqldb.SetMaxOpenConns(maxOpenConns)
		idle := maxOpenConns
		if idle > 4 {
			idle = 4 // keep a few warm; let the rest close when idle
		}
		sqldb.SetMaxIdleConns(idle)
		sqldb.SetConnMaxIdleTime(90 * time.Second)
		sqldb.SetConnMaxLifetime(30 * time.Minute)
	}

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
