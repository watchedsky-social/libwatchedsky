//go:build migrations

package migrations

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/planar"
	"github.com/pressly/goose/v3"
	"github.com/watchedsky-social/libwatchedsky/geodata"
)

func init() {
	goose.AddMigrationContext(upAddNwsData, downAddNwsData)
}

const insertNwsDataQuery = `INSERT INTO zones (oid, id, name, type, center, geometry, metadata) VALUES (?,?,?,?,ST_GeomFromWKB(?,4326),GeosMakeValid(ST_GeomFromWKB(?,4326)),?)`

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

		z := &geodata.Zone{
			ID:       fmt.Sprintf("%v", f.ID),
			Name:     f.Properties.MustString("name", ""),
			Type:     f.Properties.MustString("type", "public"),
			Metadata: geodata.JSONB(f.Properties),
			Center:   geodata.FromOrbGeometry(centroid),
			Geometry: geodata.FromOrbGeometry(f.Geometry),
		}
		z.SetOID("us")

		if _, err = stmt.ExecContext(ctx, z.OID(), z.ID, z.Name, z.Type, z.Center, z.Geometry,
			z.Metadata); err != nil {
			return err
		}
	}

	return nil
}

func downAddNwsData(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM zones")
	return err
}
