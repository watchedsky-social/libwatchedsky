package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upFixInvalidGeos, downFixInvalidGeos)
}

const fixGeometryStmt = `UPDATE zones SET geometry = GeomFromText(?, 4326) WHERE oid LIKE ?`

func upFixInvalidGeos(ctx context.Context, tx *sql.Tx) error {
	datadir, err := SourceDataRoot(ctx)
	if err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, fixGeometryStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()

	wktFiles, _ := filepath.Glob(filepath.Join(datadir, "us", "nws_zone_geojson", "manual-fixes", "*.wkt"))
	for _, file := range wktFiles {
		shortID := strings.TrimSuffix(file, ".wkt")
		wkt, ferr := os.ReadFile(file)
		if ferr != nil {
			return ferr
		}

		_, err = stmt.ExecContext(ctx, wkt, fmt.Sprintf("%%:%s", shortID))
		if err != nil {
			return err
		}
	}

	return nil
}

func downFixInvalidGeos(context.Context, *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
