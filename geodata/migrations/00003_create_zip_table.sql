-- +goose Up
-- +goose StatementBegin
CREATE TABLE us_zip_codes (
  code CHAR(5) NOT NULL PRIMARY KEY,
  name TEXT NOT NULL,
  state CHAR(2) NOT NULL,
  county_oid TEXT,
  FOREIGN KEY (county_oid) REFERENCES zones(oid) ON DELETE SET NULL
);

SELECT AddGeometryColumn('us_zip_codes', 'center', 4326, 'POINT', 'XY');

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE us_zip_codes;
-- +goose StatementEnd
