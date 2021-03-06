
-- WKT: POINT is (x,y)

-- Baildon Hill WGS84 Lat:53.856689 Lon:-1.7867456
-- POINT(-1.7867456,53.856689)  ... WGS84
-- POINT(414124.693264854 440072.398629316)   ... OSGB26

-- Identity
SELECT ST_AsText(ST_Transform(ST_GeomFromText('POINT(-1.7867456 53.856689)',4326),4326));

-- To UK OSGB36
SELECT ST_AsText(ST_Transform(ST_GeomFromText('POINT(-1.7867456 53.856689)',4326),27700));