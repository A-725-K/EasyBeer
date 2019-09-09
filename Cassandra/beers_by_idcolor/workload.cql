USE ks_user10;

EXPAND ON;

-- Remove all beers with color value less than 2
DELETE FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color < 2;

-- Retrieve all beers with color in a given range
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 6
AND color < 20;

-- ************************ --
-- CLASSIFICATION BY COLOR: --
-- ************************ --

-- Retrieve all PALE STRAW beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 2
AND color < 3;

-- Retrieve all STRAW beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 3
AND color < 4;

-- Retrieve all PALE GOLD beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 4
AND color < 6;

-- Retrieve all DEEP GOLD beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 6
AND color < 9;

-- Retrieve all PALE AMBER beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 9
AND color < 12;

-- Retrieve all MEDIUM AMBER beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 12
AND color < 15;

-- Retrieve all DEEP AMBER beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 15
AND color < 18;

-- Retrieve all AMBER BROWN beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 18
AND color < 20;

-- Retrieve all BROWN beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 20
AND color < 24;

-- Retrieve all RUBY BROWN beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 24
AND color < 30;

-- Retrieve all DEEP BROWN beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 30
AND color < 40;

-- Retrieve all BLACK beers
SELECT * FROM beers_by_idcolor
WHERE clusteringid IN (1,2,3)
AND color >= 40;

-- Get the maximum, minimum and average color by volume among all beers
SELECT MIN(color) AS "Minimum Color", 
	   MAX(color) AS "Maximum Color", 
	   AVG(color) AS "Average Color"
FROM beers_by_idcolor;
