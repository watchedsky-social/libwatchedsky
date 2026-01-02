//go:build migrations

package migrations

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jghiloni/go-commonutils/v3/slices"
	"github.com/jghiloni/go-commonutils/v3/values"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
	"github.com/pressly/goose/v3"
	"github.com/watchedsky-social/libwatchedsky/geodata"
)

func init() {
	goose.AddMigrationContext(upAddZipCodeData, downAddZipCodeData)
}

const (
	prefixPrefix = `oid:ws:us:`
	prefixSuffix = `:county:`
	prefixLen    = len(prefixPrefix) + len(prefixSuffix) + 2

	getZones               = `SELECT oid, AsEWKB(geometry) as geo FROM zones WHERE type = 'county'`
	insertZipCodeDataQuery = `INSERT INTO us_zip_codes (code, name, state, county_oid, center) VALUES (?, ?, ?, ?, ST_GeomFromWKB(?,4326))`
)

type zipCodeData struct {
	code      string
	name      string
	state     string
	countyOID string
	center    *geodata.Geometry
}

type countyRow struct {
	oid      string
	geometry geodata.Geometry
}

func upAddZipCodeData(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, getZones)
	if err != nil {
		return err
	}
	defer rows.Close()

	zoneMap := map[string][]countyRow{}

	stmt, err := tx.PrepareContext(ctx, insertZipCodeDataQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var row countyRow
		if err = rows.Scan(&row.oid, &row.geometry); err != nil {
			return err
		}

		key := strings.ToLower(row.oid[:prefixLen])
		zoneMap[key] = append(zoneMap[key], row)
	}

	zips, err := readZipCodes(ctx)
	if err != nil {
		return err
	}

	for i := range zips {
		key := strings.ToLower(fmt.Sprintf("%s%s%s", prefixPrefix, zips[i].state, prefixSuffix))
		stateZones, exists := zoneMap[key]
		if !exists {
			continue
		}

		j := slices.IndexFunc(stateZones, func(row countyRow) bool {
			og := row.geometry.AsOrbGeometry()
			zipCenterOG := zips[i].center.AsOrbGeometry()
			switch g := og.(type) {
			case orb.Polygon:
				return planar.PolygonContains(g, zipCenterOG.(orb.Point))
			case orb.MultiPolygon:
				return planar.MultiPolygonContains(g, zipCenterOG.(orb.Point))
			default:
				return false
			}
		})

		countyOID := sql.NullString{}
		if j != -1 {
			countyOID = sql.NullString{Valid: true, String: stateZones[j].oid}
		}

		if _, err = stmt.ExecContext(ctx, zips[i].code, zips[i].name, zips[i].state, countyOID, zips[i].center); err != nil {
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
