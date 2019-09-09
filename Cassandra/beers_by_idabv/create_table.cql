USE ks_user10;

DROP TABLE IF EXISTS beers_by_idabv;

CREATE TABLE beers_by_idabv (
	ClusteringId int,
	ABV float,
	Name text,
	Style text,
	PRIMARY KEY(ClusteringId, ABV, Name, Style)
);
