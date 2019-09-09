USE ks_user10;

EXPAND ON;

-- Retrieve all beers whose name starts with a given string
SELECT * 
FROM beers_by_all
WHERE name LIKE 'Summer%';

-- Retrieve all beers whose style contains a given string
SELECT * 
FROM beers_by_all
WHERE style LIKE '%Exp%';
