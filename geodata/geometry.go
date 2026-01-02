package geodata

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
)

// Geometry wraps [github.com/paulmach/orb.Geometry] with other interfaces that
// improves its serialization and deserialization abilities
type Geometry struct {
	g orb.Geometry
}

// FromOrbGeometry creates and returns a new [Geometry] from a [github.com/paulmach/orb.Geometry]
// or nil if g is nil
func FromOrbGeometry(g orb.Geometry) *Geometry {
	if g == nil {
		return nil
	}

	return &Geometry{g: g}
}

// AsOrbGeometry returns the underlying [github.com/paulmach/orb.Geometry]
func (g *Geometry) AsOrbGeometry() orb.Geometry {
	return g.g
}

// Scan implements [database/sql.Scanner]
func (g *Geometry) Scan(src any) error {
	var og orb.Geometry
	s := wkb.Scanner(og)

	if src == nil {
		return nil
	}

	var err error
	switch srcBytes := src.(type) {
	case []byte:
		err = s.Scan(srcBytes)
	case string:
		var b []byte
		b, err = hex.DecodeString(srcBytes)
		if err != nil {
			return err
		}
		err = s.Scan(b)
	default:
		err = fmt.Errorf("need []byte for Scan, got %T", src)
	}

	if err != nil {
		return err
	}

	if !s.Valid {
		return errors.New("invalid WKB returned")
	}

	ng := FromOrbGeometry(og)
	if ng == nil {
		return errors.New("underlying geometry is nil")
	}

	*g = *ng

	return nil
}

// Value implements [database/sql/driver.Valuer]
func (g *Geometry) Value(src any) (driver.Value, error) {
	return wkb.Value(g.g).Value()
}

// MarshalJSON implements [encoding/json.Marshaler]
func (g *Geometry) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.g)
}

// UnmarshalJSON implements [encoding/json.Unmarshaler]
func (g *Geometry) UnmarshalJSON(data []byte) error {
	var og orb.Geometry

	if err := json.Unmarshal(data, &og); err != nil {
		return err
	}

	ng := FromOrbGeometry(og)
	if ng == nil {
		return errors.New("underlying geometry is nil")
	}

	*g = *ng
	return nil
}
