CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS crimes(
    crimeID VARCHAR,
    districtName VARCHAR,
    latitude GEOMETRY,
    longitude GEOMETRY,
    crimeType VARCHAR,
    lastOutcome VARCHAR
    PRIMARY KEY(crimeID)
);