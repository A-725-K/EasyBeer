USE ks_user10;

DROP TABLE IF EXISTS beers_by_idibu;

CREATE TABLE beers_by_idibu (
	ClusteringId int,
	IBU float,
	Name text,
	Style text,
	PRIMARY KEY(ClusteringId, IBU, Name, Style)
);
