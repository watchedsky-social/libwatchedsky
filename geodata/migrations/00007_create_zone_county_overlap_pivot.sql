-- +goose Up
-- +goose StatementBegin
CREATE TABLE zone_county_pivot
(
  zone_oid   TEXT NOT NULL,
  county_oid TEXT NOT NULL,
  PRIMARY KEY (zone_oid, county_oid),
  FOREIGN KEY (zone_oid) REFERENCES zones (oid) ON DELETE CASCADE,
  FOREIGN KEY (county_oid) REFERENCES zones (oid) ON DELETE CASCADE
);

INSERT INTO zone_county_pivot
SELECT z1.oid AS zone_oid, z2.oid AS county_oid
FROM zones z1
       INNER JOIN zones z2 ON z1.geometry IS NOT NULL AND z2.geometry IS NOT NULL AND
                              ST_Intersects(z1.geometry, z2.geometry)
WHERE z1.type != 'county' AND z2.type = 'county';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE zone_county_pivot;
-- +goose StatementEnd
