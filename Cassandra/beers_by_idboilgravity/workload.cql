USE ks_user10;

EXPAND ON;

-- Delete beers with boilgravity out of range
DELETE FROM beers_by_idboilgravity
WHERE clusteringid IN (1,2,3)
AND boilgravity >= 100;

DELETE FROM beers_by_idboilgravity
WHERE clusteringid IN (1,2,3)
AND boilgravity < 3;

-- Show all the analcoholic beers
SELECT * FROM beers_by_idboilgravity
WHERE clusteringid IN (1,2,3)
AND boilgravity >= 3
AND boilgravity < 8;

-- Show all the light beers
SELECT * FROM beers_by_idboilgravity
WHERE clusteringid IN (1,2,3)
AND boilgravity >= 8
AND boilgravity < 10.5;

-- Show all the normal beers
SELECT * FROM beers_by_idboilgravity
WHERE clusteringid IN (1,2,3)
AND boilgravity >= 10.5
AND boilgravity < 12.5;

-- Show all the special beers
SELECT * FROM beers_by_idboilgravity
WHERE clusteringid IN (1,2,3)
AND boilgravity >= 12.5
AND boilgravity < 14.5;

-- Show all the analcoholic beers
SELECT * FROM beers_by_idboilgravity
WHERE clusteringid IN (1,2,3)
AND boilgravity >= 14.5;

