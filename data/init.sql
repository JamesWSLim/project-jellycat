CREATE TABLE jellycat_data (
    Jellycat_name TEXT,
    Price DECIMAL,
    Information TEXT,
    Link TEXT,
    Image_link TEXT,
    Date_created timestamp,
    PRIMARY KEY (Jellycat_name, Date_created)
);

COPY jellycat_data
FROM '/docker-entrypoint-initdb.d/jellycat.csv'
DELIMITER ','
CSV HEADER;