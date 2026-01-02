//go:build migrations

package migrations

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/jghiloni/go-commonutils/v3/values"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/planar"
	"github.com/pressly/goose/v3"
	"github.com/watchedsky-social/geodata/db"
)

func init() {
	goose.AddMigrationContext(upAddNwsData, downAddNwsData)
}

const insertNwsDataQuery = `INSERT INTO zones (oid, id, name, type, center, geometry, metadata) VALUES (?,?,?,?,ST_GeomFromWKB(?,4326),ST_GeomFromWKB(?,4326),?)`

func upAddNwsData(ctx context.Context, tx *sql.Tx) error {
	datadir, err := SourceDataRoot(ctx)
	if err != nil {
		return err
	}

	fp, err := os.Open(filepath.Join(datadir, "us", "nws_zone_geojson", "all.json"))
	if err != nil {
		return err
	}
	defer fp.Close()

	decoder := json.NewDecoder(fp)
	if _, err = decoder.Token(); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, insertNwsDataQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for decoder.More() {
		var f geojson.Feature
		if err = decoder.Decode(&f); err != nil {
			return err
		}

		centroid, _ := planar.CentroidArea(f.Geometry)

		if _, err = stmt.ExecContext(ctx, values.Must(db.OID("us", f.ID.(string), f.Properties)), f.ID,
			f.Properties.MustString("name", ""), f.Properties.MustString("type", "public"),
			db.Geometry{G: centroid}, db.Geometry{G: f.Geometry}, db.JSONB(f.Properties)); err != nil {
			return err
		}
	}

	return nil
}

func downAddNwsData(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM zones")
	return err
}
