//go:build migrations

package migrations

import (
	"context"
	"database/sql"
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jghiloni/go-commonutils/v3/slices"
	"github.com/jghiloni/go-commonutils/v3/values"
	"github.com/paulmach/orb"
	"github.com/pressly/goose/v3"
	"github.com/watchedsky-social/libwatchedsky/geodata"
)

func init() {
	goose.AddMigrationContext(upAddZipCodeData, downAddZipCodeData)
}

const (
	insertZipCodeDataQuery = `INSERT INTO us_zip_codes (code, name, state, center) VALUES (?, ?, ?, ST_GeomFromWKB(?,4326))`
)

type zipCodeData struct {
	code      string
	name      string
	state     string
	countyOID string
	center    *geodata.Geometry
}

func upAddZipCodeData(ctx context.Context, tx *sql.Tx) error {
	stmt, err := tx.PrepareContext(ctx, insertZipCodeDataQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	zips, err := readZipCodes(ctx)
	if err != nil {
		return err
	}

	for i := range zips {
		if _, err = stmt.ExecContext(ctx, zips[i].code, zips[i].name, zips[i].state, zips[i].center); err != nil {
			return err
		}
	}

	return err
}

func downAddZipCodeData(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM us_zip_codes")
	return err
}

func readZipCodes(ctx context.Context) ([]zipCodeData, error) {
	datadir, err := SourceDataRoot(ctx)
	if err != nil {
		return nil, err
	}

	fp, err := os.Open(filepath.Join(datadir, "us", "zip_code_database.csv"))
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	csvReader := csv.NewReader(fp)

	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	// ditch the header row
	records = records[1:]

	records = slices.Filter(records, func(record []string) bool {
		return record[1] == "STANDARD"
	})

	return slices.Map(records, func(record []string) zipCodeData {
		lon, lat := values.Must(strconv.ParseFloat(record[10], 64)), values.Must(strconv.ParseFloat(record[9], 64))

		return zipCodeData{
			code:      record[0],
			name:      record[2],
			state:     record[5],
			countyOID: "",
			center:    geodata.FromOrbGeometry(orb.Point{lon, lat}),
		}
	}), nil
}
