//go:build migrations

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
	_ "github.com/watchedsky-social/go-spatialite"
)

// GooseCommand executes a standard goose command with a SQLite3 DB path, a directory
// containing the source data, and an arbitrary list of arguments to the command
type GooseCommand func(ctx context.Context, dbPath, dataDir string,
	cmdArgs ...string) error

var (
	Up       = commonCommand("up")
	UpByOne  = commonCommand("up-by-one")
	Down     = commonCommand("down")
	Redo     = commonCommand("redo")
	Reset    = commonCommand("reset")
	Status   = commonCommand("status")
	Version  = commonCommand("version")
	Fix      = commonCommand("fix")
	Validate = commonCommand("validate")
)

func commonCommand(cmdName string) GooseCommand {
	return func(ctx context.Context, dbPath, dataDir string, cmdArgs ...string) error {
		if err := goose.SetDialect("sqlite"); err != nil {
			return err
		}

		dsn := fmt.Sprintf("file:%s", dbPath)
		db, err := sql.Open("spatialite", dsn)
		if err != nil {
			return err
		}

		ctx, err = SetSourceDataRoot(ctx, dataDir)
		return goose.RunContext(ctx, cmdName, db, ".")
	}
}
