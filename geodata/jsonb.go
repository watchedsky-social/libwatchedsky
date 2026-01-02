package geodata

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/paulmach/orb/geojson"
)

// JSONB is a map[string]any that implements [database/sql.Scanner] and [database/sql/driver.Valuer]
// which represents a binary encoding of JSON for databases
type JSONB geojson.Properties

// Scan implements [database/sql.Scanner]
func (j *JSONB) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	result := JSONB{}
	err := json.Unmarshal(bytes, &result)
	*j = result
	return err
}

// Value implements [database/sql/driver.Valuer]
func (j JSONB) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

// MustBool exposes the [geojson.Properties.MustBool] func
func (j JSONB) MustBool(key string, def ...bool) bool {
	return geojson.Properties(j).MustBool(key, def...)
}

// MustInt exposes the [geojson.Properties.MustInt] func
func (j JSONB) MustInt(key string, def ...int) int {
	return geojson.Properties(j).MustInt(key, def...)
}

// MustFloat64 exposes the [geojson.Properties.MustFloat64] func
func (j JSONB) MustFloat64(key string, def ...float64) float64 {
	return geojson.Properties(j).MustFloat64(key, def...)
}

// MustString exposes the [geojson.Properties.MustString] func
func (j JSONB) MustString(key string, def ...string) string {
	return geojson.Properties(j).MustString(key, def...)
}
