CREATE TABLE jellycat (
    jellycat_id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    jellycat_name TEXT,
    price DECIMAL,
    information TEXT,
    link TEXT,
    image_link TEXT,
    date_created timestamp
);

CREATE TABLE size (
    jellycat_size_id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    jellycat_id uuid,
    size TEXT,
    price DECIMAL,
    stock TEXT,
    CONSTRAINT jellycat_size_id_fk FOREIGN KEY(jellycat_id) REFERENCES jellycat(jellycat_id)
);