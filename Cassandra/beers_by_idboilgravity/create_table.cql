USE ks_user10;

DROP TABLE IF EXISTS beers_by_idboilgravity;

CREATE TABLE beers_by_idboilgravity (
	ClusteringId int,
	BoilGravity float,
	Style text,
	Name text,
	ABV float,
	IBU float,
	Color float,
	BrewMethod text,
	PRIMARY KEY(ClusteringId, BoilGravity, Style, Name)
);
