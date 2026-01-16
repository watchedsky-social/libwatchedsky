package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddCountiesToZipData, downAddCountiesToZipData)
}

const (
	getCounty     = `SELECT oid FROM zones WHERE type = 'county' AND Contains(geometry, (SELECT center FROM us_zip_codes where code = ?)) LIMIT 1`
	updateZipCode = `UPDATE us_zip_codes SET county_oid = ? WHERE code = ?`
)

func upAddCountiesToZipData(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	zips, err := readZipCodes(ctx)

	getCountyStmt, err := tx.PrepareContext(ctx, getCounty)
	if err != nil {
		return err
	}
	defer getCountyStmt.Close()

	updateStmt, err := tx.PrepareContext(ctx, updateZipCode)
	if err != nil {
		return err
	}
	defer updateStmt.Close()

	for i := range zips {
		zip := zips[i].code

		countyOIDResult, ferr := getCountyStmt.QueryContext(ctx, zip)
		if ferr != nil {
			return ferr
		}

		var countyOID string
		if countyOIDResult.Next() {
			countyOIDResult.Scan(&countyOID)
		}
		countyOIDResult.Close()

		if countyOID != "" {
			if _, err = updateStmt.ExecContext(ctx, countyOID, zip); err != nil {
				return err
			}
		}
	}

	return nil
}

func downAddCountiesToZipData(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	_, err := tx.ExecContext(ctx, `UPDATE us_zip_codes SET county_oid = NULL`)
	return err
}
