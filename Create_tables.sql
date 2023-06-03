CREATE TABLE phonepe_pulse.aggregated_transaction (
   id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
   state TEXT,
   year INTEGER,
   quarter INTEGER,
   transaction_type TEXT,
   transaction_count INTEGER,
   transaction_amount FLOAT
);
CREATE TABLE phonepe_pulse.map_transaction (
   id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
   state TEXT,
   year INTEGER,
   quarter INTEGER,
   transaction_type TEXT,
   transaction_count INTEGER,
   transaction_amount FLOAT
);
CREATE TABLE phonepe_pulse.top_transaction (
   id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
   state TEXT,
   year INTEGER,
   quarter INTEGER,
   transaction_type TEXT,
   transaction_count INTEGER,
   transaction_amount FLOAT
);
CREATE TABLE phonepe_pulse.aggregated_user (
   id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
   state TEXT,
   year INTEGER,
   Quarter INTEGER,
   brand TEXT,
   brand_count INTEGER,
   brand_percentage FLOAT
   );

CREATE TABLE phonepe_pulse.map_user (
	id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
    state TEXT,
    year INTEGER,
	quarter INTEGER,
	district TEXT,
	registered_users INTEGER,
	app_opening INTEGER
);

CREATE TABLE phonepe_pulse.top_user (
	id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
	state TEXT,
	year INTEGER,
	quarter INTEGER,
	district TEXT,
	registered_users INTEGER
);
