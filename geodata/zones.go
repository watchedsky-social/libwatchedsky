package geodata

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
)

// Zone represents a geographic zone in the zones table
type Zone struct {
	oid      string
	ID       string
	Name     string
	Type     string
	Metadata JSONB
	Center   *Geometry
	Geometry *Geometry
}

// TypeaheadResult are data returned when a user uses typeahead methods
type TypeaheadResult struct {
	OID      string `json:"oid"`
	Input    string `json:"input"`
	FullText string `json:"fulltext"`
}

const oidTemplate = "oid:ws:%s:%s:%s:%s"

var (
	// ErrCannotGetOID is returned if necessary information is missing or incalculable
	ErrCannotGetOID = errors.New("cannot get oid")
)

// SetOID generates a watchedsky Object ID for a feature and sets it on the struct. It is of the form
//
//	oid:ws:<country>:<state/province>:<feature type>:<short id>
//
// In this case, country is the ISO 3166 Alpha-2 2 character code (in lower case). State/Province is the
// official abbreviation for that country's "top level" political subdivision. For example, in the US, this
// is state, in Canada it is province, etc. Feature type is defined by the source of the data. For the US,
// this is either "coastal", "county", "fire", "offshore", or "public". Finally, the short ID is the ID of the
// feature, with any leading URL parts removed if they exist. If the OID cannot be generated, it returns an
// error
//
// Example: Cuyahoga County, Ohio (home of Case Western Reserve University, my alma mater) has the oid of
//
//	oid:ws:us:oh:county:OHC035
func (z *Zone) SetOID(country string) error {
	country = strings.ToLower(country)
	u, err := url.Parse(z.ID)
	id := z.ID
	if err == nil {
		id = path.Base(u.Path)
	}

	stprov := ""
	switch country {
	case "us":
		stprov, _ = z.Metadata.GetString("state")
	}

	if stprov == "" {
		stprov = "xx"
	}

	ftype, _ := z.Metadata.GetString("type")
	if ftype == "" {
		return fmt.Errorf("%w: cannot determine feature type for %s", ErrCannotGetOID, id)
	}

	z.oid = fmt.Sprintf(oidTemplate, country, strings.ToLower(stprov), strings.ToLower(ftype), id)
	return nil
}

func (z *Zone) OID() string {
	return z.oid
}
