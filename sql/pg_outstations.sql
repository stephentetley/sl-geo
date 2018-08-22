-- Must have PostGIS extensions enabled
-- At the prompt:
-- some_db=# \i d:/path_to/sql/pg_outstations.sql

DROP TABLE spt_outstations;


-- This is a sample table for running ST_DWithin queries
-- We need geom, all other fields are for printing  and identification.

-- Always use Geography (rather than Geometry) unless we have a specific 
-- use-case (i.e. input data might not be geodic).
-- Geography accounts for the curvature of the earth (vis Haversine distance)

CREATE TABLE spt_outstations (
    os_name VARCHAR(460) PRIMARY KEY,
    od_name VARCHAR(20), 
    osgb36_text VARCHAR(20), 
    set_name VARCHAR(16),
    parent_ou VARCHAR(40),
    parent_ou_comment VARCHAR(80),
    os_location geography (POINT)
);



