-- create the 4 permanent tables
CREATE TABLE mapping (
  tld VARCHAR(15),
  description VARCHAR(200) NOT NULL,
  PRIMARY KEY (tld)
);

CREATE TABLE domain (
  domain_name VARCHAR(50),
  PRIMARY KEY (domain_name)
);

CREATE TABLE tld (
  tld_id INT,
	tld1 VARCHAR(15) NOT NULL,
	tld2 VARCHAR(15),
	PRIMARY KEY (tld_id)
);

CREATE TABLE url (
  domain_name VARCHAR(50) NOT NULL,
  tld_id INT NOT NULL,
  position INT NOT NULL,
  PRIMARY KEY (domain_name, tld_id),
  FOREIGN KEY (domain_name) REFERENCES domain,
  FOREIGN KEY (tld_id) REFERENCES tld,
  UNIQUE (position),
  CHECK (position >= 1 AND position <= 10000)
);

-- insert values into mapping table
INSERT INTO mapping (tld, description)
VALUES (?, ?)
ON CONFLICT DO NOTHING;

-- create a temporary table to put urls in
CREATE TEMP TABLE url_temp (
  pos INT,
  domain_name VARCHAR(50),
  tld1 VARCHAR(15) NOT NULL,
  tld2 VARCHAR(15)
);

-- insert data into temp url table
INSERT INTO url_temp (pos, domain_name, tld1, tld2)
VALUES (?, ?, ?, ?)

-- insert tld data from url temp table into real url table
INSERT INTO tld
  SELECT row_number() OVER (ORDER BY min(pos)), tld1, tld2
  FROM url_temp
  GROUP BY tld1, tld2
ON CONFLICT DO NOTHING;

-- insert domain_name data from url temp table into real url table
INSERT INTO domain
  SELECT domain_name
  FROM url_temp
  GROUP BY domain_name
  ORDER BY domain_name
ON CONFLICT DO NOTHING;

INSERT INTO url (position, domain_name, tld_id)
VALUES (?, ?, ?);

-- shows top 10 urls ordered by position
CREATE VIEW top_10_urls AS
  SELECT position, domain_name, tld1, tld2
  FROM url
  NATURAL JOIN tld
  ORDER BY position
  LIMIT 10;

  -- shows top 10 distinct tlds ordered by popularity (best position for each tld)
  CREATE VIEW top_10_tlds AS
    SELECT min(position) AS best_position, tld1, tld2, description
    FROM url
    NATURAL JOIN tld, mapping
    WHERE
    (
      CASE
        WHEN tld.tld2 = '' THEN tld.tld1
        ELSE tld.tld2
      END
    )  = mapping.tld
    GROUP BY tld1, tld2, description
    ORDER BY best_position
    LIMIT 10;

-- shows top 10 domains which appear more than once in list, ordered by popularity (best position for each tld)
CREATE VIEW top_10_repeated_domains AS
  SELECT min(position) AS best_position, domain_name
  FROM url
  GROUP BY domain_name
  HAVING count(*) > 1
  ORDER BY best_position
  LIMIT 10;

  -- Q1 - 10 most popular URLs in descending order of popularity
  SELECT *
  FROM top_10_urls
  ORDER BY position DESC;

  -- Q2 - 10 distinct most popular top level domains in descending order of popularity

  SELECT tld1, tld2
  FROM top_10_tlds
  ORDER BY best_position DESC;

  -- Q3 - 10 distinct most popular descriptions of the righmost part of tld in desc
  -- order of puplarity

  SELECT description
  FROM top_10_tlds
  ORDER BY best_position DESC;

  -- Q4 - top 10 distinct domain names that appear more than once,
   -- ordered by popularity

  SELECT domain_name
  FROM top_10_repeated_domains;
