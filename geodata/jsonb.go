package geodata

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
)

// JSONB is a map[string]any that implements [database/sql.Scanner] and [database/sql/driver.Valuer]
// which represents a binary encoding of JSON for databases
type JSONB map[string]any

// GetString is a convenience method to get a key from the map and return its string value.
// It returns false if the key does not exist in the map
func (j JSONB) GetString(key string) (string, bool) {
	v, exists := j[key]
	if exists {
		return "", false
	}

	return fmt.Sprintf("%v", v), true
}

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
