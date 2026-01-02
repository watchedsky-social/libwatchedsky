-- +goose Up
-- +goose StatementBegin
PRAGMA FOREIGN_KEYS = ON;

SELECT InitSpatialMetadata();

CREATE TABLE zones (
    oid TEXT NOT NULL PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    metadata BLOB DEFAULT NULL
);

SELECT AddGeometryColumn('zones', 'center', 4326, 'POINT', 'XY');
SELECT AddGeometryColumn('zones', 'geometry', 4326, 'GEOMETRY', 'XY');

CREATE UNIQUE INDEX ui_zones_id ON zones(id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX ui_zones_id;
DROP TABLE zones;
-- +goose StatementEnd
