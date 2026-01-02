//go:build migrations && fts5

package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jghiloni/go-commonutils/v3/slices"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upTypeaheadFulltextData, downTypeaheadFulltextData)
}

const (
	createFulltextTable = `CREATE VIRTUAL TABLE typeahead_index USING fts5(display_string, state_province_code, oid UNINDEXED)`
	getCounties         = `SELECT oid, name, JSONB_EXTRACT('$.state') AS state FROM zones WHERE type = 'county'`
	getCities           = `SELECT code, name, state, county_oid FROM us_zip_codes`
	insertEntry         = `INSERT INTO typeahead_index (display_string, state_province_code, oid) VALUES (?, ?, ?)`

	// "<county name> (county/parish), <state>"
	usCountyDisplayTemplate = `%s%s, %s, United States`
	// "<city name>, <county> (county/parish), <zip>, United States"
	usCityDisplayTemplate = `%s, %s%s, %s, United States`
)

var (
	stateCodeMap = map[string]string{
		"AK": "Alaska",
		"AL": "Alabama",
		"AR": "Arkansas",
		"AS": "American Samoa",
		"AZ": "Arizona",
		"CA": "California",
		"CO": "Colorado",
		"CT": "Connecticut",
		"DC": "Washington, DC",
		"DE": "Delaware",
		"FL": "Florida",
		"FM": "Federated States of Micronesia",
		"GA": "Georgia",
		"GU": "Guam",
		"HI": "Hawaii",
		"IA": "Iowa",
		"ID": "Idaho",
		"IL": "Illinois",
		"IN": "Indiana",
		"KS": "Kansas",
		"KY": "Kentucky",
		"LA": "Louisiana",
		"MA": "Massachusetts",
		"MD": "Maryland",
		"ME": "Maine",
		"MH": "Marshall Islands",
		"MI": "Michigan",
		"MN": "Minnesota",
		"MO": "Missouri",
		"MP": "Northern Mariana Islands",
		"MS": "Mississippi",
		"MT": "Montana",
		"NC": "North Carolina",
		"ND": "North Dakota",
		"NE": "Nebraska",
		"NH": "New Hampshire",
		"NJ": "New Jersey",
		"NM": "New Mexico",
		"NV": "Nevada",
		"NY": "New York",
		"OH": "Ohio",
		"OK": "Oklahoma",
		"OR": "Oregon",
		"PA": "Pennsylvania",
		"PR": "Puerto Rico",
		"PW": "Palau",
		"RI": "Rhode Island",
		"SC": "South Carolina",
		"SD": "South Dakota",
		"TN": "Tennessee",
		"TX": "Texas",
		"UT": "Utah",
		"VA": "Virginia",
		"VI": "US Virgin Islands",
		"VT": "Vermont",
		"WA": "Washington",
		"WI": "Wisconsin",
		"WV": "West Virginia",
		"WY": "Wyoming",
	}

	nonStateCodes = []string{"AS", "FM", "GU", "MH", "MP", "PR", "PW", "VI"}

	oidResultMap = map[string]countyResult{}
)

type countyResult struct {
	oid   string
	name  string
	state sql.NullString
}

func (c countyResult) String() string {
	state := c.state.String
	return fmt.Sprintf(usCountyDisplayTemplate, c.name, getCountyTerm(state), stateCodeMap[strings.ToUpper(state)])
}

type cityResult struct {
	zip       string
	name      string
	state     sql.NullString
	countyOID sql.NullString
}

func (c cityResult) String() string {
	county := oidResultMap[c.countyOID.String]
	state := c.state.String

	return fmt.Sprintf(usCityDisplayTemplate, c.name, county.name, getCountyTerm(state), c.zip)
}

func upTypeaheadFulltextData(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, createFulltextTable); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, insertEntry)
	if err != nil {
		return err
	}
	defer stmt.Close()

	countyResults, err := tx.QueryContext(ctx, getCounties)
	if err != nil {
		return err
	}
	defer countyResults.Close()

	for countyResults.Next() {
		var c countyResult
		if err = countyResults.Scan(&c.oid, &c.name, &c.state); err != nil {
			return err
		}

		if _, err = stmt.ExecContext(ctx, c.String(), strings.ToUpper(c.state.String),
			c.oid); err != nil {
			return err
		}

		oidResultMap[c.oid] = c
	}

	cityResults, err := tx.QueryContext(ctx, getCities)
	if err != nil {
		return err
	}
	defer cityResults.Close()

	for cityResults.Next() {
		var c cityResult
		if err = cityResults.Scan(&c.zip, &c.name, &c.state, &c.countyOID); err != nil {
			return err
		}

		if _, err = stmt.ExecContext(ctx, c.String(), strings.ToUpper(c.state.String),
			c.countyOID); err != nil {
			return err
		}
	}

	return nil
}

func downTypeaheadFulltextData(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP TABLE typeahead_index`)
	return err
}

func getCountyTerm(state string) string {
	cty := " County"
	switch {
	case strings.EqualFold(state, "la"):
		cty = " Parish"
	case slices.Contains(nonStateCodes, strings.ToUpper(state)):
		cty = ""
	}

	return cty
}
