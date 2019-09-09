USE ks_user10;

DROP TABLE IF EXISTS beers_by_style;

CREATE TABLE beers_by_style (
	Style text,
	Name text,
	BrewMethod text,
	IBU float,
	ABV float,
	Color float,
	PRIMARY KEY(Style, Name)
);
