USE ks_user10;

EXPAND ON;

-- Remove all beers with alcohol by volume less than 2°
DELETE FROM beers_by_idabv
WHERE clusteringid IN (1,2,3)
AND abv <= 2;

-- Retrieve all beers with alcohol by volume in a given range
SELECT * FROM beers_by_idabv
WHERE clusteringid IN (1,2,3)
AND abv >= 5
AND abv < 6;

-- Get the maximum, minimum and average value of alcohol by volume among all beers
SELECT MIN(abv) AS "Minimum ABV", 
	   MAX(abv) AS "Maximum ABV", 
	   AVG(abv) AS "Average ABV"
FROM beers_by_idabv;
