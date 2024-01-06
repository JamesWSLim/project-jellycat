CREATE TABLE jellycat (
    jellycatid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    jellycatname TEXT,
    category TEXT,
    link TEXT,
    imagelink TEXT,
    date_created timestamp
);

CREATE TABLE size (
    jellycatsizeid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    jellycatid uuid,
    size TEXT,
    price DECIMAL,
    stock TEXT,
    CONSTRAINT jellycatsizeidfk FOREIGN KEY(jellycatid) REFERENCES jellycat(jellycatid)
);