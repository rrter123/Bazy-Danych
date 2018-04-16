--DEKLARACJE RELACJI, SEKWENCJI I DOMEN
CREATE TABLE wydarzenie (
	nazwa varchar CONSTRAINT idwyd PRIMARY KEY,
	poczatek timestamp NOT NULL,
	koniec timestamp NOT NULL

	);
CREATE TABLE uczestnik (
	login varchar CONSTRAINT iducz PRIMARY KEY,
	haslo varchar NOT NULL,
	imie varchar,
	nazwisko varchar,
	organizator BOOLEAN DEFAULT FALSE
	);

CREATE TABLE referat (
	id varchar CONSTRAINT idref PRIMARY KEY,
	autor varchar REFERENCES uczestnik,
	wydarzenieid varchar REFERENCES wydarzenie,
	temat varchar,
	poczatek timestamp,
	sala integer
	);
	
	
CREATE TABLE wezmie_udzial (
	wydarzenieid varchar REFERENCES wydarzenie,
	uczestnikid varchar REFERENCES uczestnik
	);
CREATE DOMAIN oceny AS integer
	CHECK( VALUE>0 AND VALUE<11
	);
CREATE TABLE ocena (
	uczestnikid varchar REFERENCES uczestnik,
	referatid varchar REFERENCES referat,
	ocena oceny
	);
	
CREATE TABLE obecnosc (
	referatid varchar REFERENCES referat,
	uczestnikid varchar REFERENCES uczestnik
	);
	
CREATE TABLE znajomosc (
	znajomy1 varchar references uczestnik,
	znajomy2 varchar references uczestnik
	);
	
--KONIEC DEKLARACJI RELACJI

CREATE OR REPLACE FUNCTION wydarzenia(text, timestamp, timestamp)
RETURNS text
AS $X$
BEGIN
	IF (($2) < ($3)) 
		THEN 
			INSERT INTO wydarzenie VALUES ($1,$2,$3);
			RETURN 'OK';
		ELSE RETURN 'ERROR';
	END IF;
END;
$X$ LANGUAGE plpgsql;



--Wyglaszanie referatow

CREATE OR REPLACE FUNCTION prepare(varchar, int, varchar)
RETURNS VOID 
AS $X$
BEGIN
	IF (NOT EXISTS (SELECT * FROM referat WHERE id = $1))
	THEN INSERT INTO referat (id) VALUES ($1);
	END IF;
	IF (NOT EXISTS (SELECT * FROM ocena where uczestnikid=$3 and referatid = $1))
	THEN INSERT INTO ocena VALUES ($3, $1, $2);
	END IF;
END;
$X$ LANGUAGE plpgsql;

