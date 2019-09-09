USE ks_user10;

DROP TABLE IF EXISTS beers_by_idcolor;

CREATE TABLE beers_by_idcolor (
	ClusteringId int,
	Color float,
	Name text,
	Style text,
	PRIMARY KEY(ClusteringId, Color, Name, Style)
);
