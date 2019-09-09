USE ks_user10;

EXPAND ON;

-- Remove all beers with international bitterness unit smaller than 1
DELETE FROM beers_by_idibu
WHERE clusteringid IN (1,2,3)
AND ibu < 1;

-- Remove all beers with international bitterness unit grater than 150
DELETE FROM beers_by_idibu
WHERE clusteringid IN (1,2,3)
AND ibu >= 150;

-- Retrieve all beers with international bitterness unit in a given range
SELECT * FROM beers_by_idibu
WHERE clusteringid IN (1,2,3)
AND ibu >= 10
AND ibu < 20;

-- Get the maximum, minimum and average international bitterness unit among all beers
SELECT MIN(ibu) AS "Minimum IBU", 
	   MAX(ibu) AS "Maximum IBU", 
	   AVG(ibu) AS "Average IBU"
FROM beers_by_idibu;
