//go:build migrations

package migrations

import "embed"

// SQLMigrations contains the pure SQL goose migrations
//
//go:embed *.sql
var SQLMigrations embed.FS
