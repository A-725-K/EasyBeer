USE ks_user10;

EXPAND ON;

-- Given a style retrieve the average alcohol by volume
SELECT AVG(abv) AS "Average ABV"
FROM beers_by_style
WHERE style = 'Witbier';

-- Given a style show the lightest and the darkest color
SELECT MIN(color) AS "MIN Color", MAX(color) AS "MAX Color"
FROM beers_by_style
WHERE style = 'Saison';

-- Show in alphabetical order the beers of a given style
SELECT name, style, abv, ibu 
FROM beers_by_style
WHERE style = 'American Pale Ale'
ORDER BY name;

-- Count how many beers share a given style
SELECT COUNT(*) AS "Total"
FROM beers_by_style
WHERE style = 'Blonde Ale';

-- Show all styles present in the dataset
SELECT DISTINCT Style
FROM beers_by_style;
