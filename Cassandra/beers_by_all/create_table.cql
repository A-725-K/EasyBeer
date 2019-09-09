USE ks_user10;

DROP TABLE IF EXISTS beers_by_all;

CREATE TABLE beers_by_all (
	ClusteringId int,
	Size float,
	OG float,
	FG float,
	ABV float,
	IBU float,
	Color float,
	BoilSize float,
	BoilTime float,
	BoilGravity float,
	Efficiency float,
	BrewMethod text,
	PrimaryTemp float,
	Name text,
	Style text,
	PRIMARY KEY(ClusteringId, Size, OG, FG, ABV, IBU, Color, BoilSize, BoilTime, BoilGravity, Efficiency, BrewMethod, PrimaryTemp)
);

CREATE CUSTOM INDEX name_index  ON beers_by_all (Name)  USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS'};
CREATE CUSTOM INDEX style_index ON beers_by_all (Style) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS'};
