CREATE TABLE requests (
        id serial PRIMARY KEY,
        query text NOT NULL,
        locationId integer NOT NULL,
        UNIQUE (query, locationId)
    );

CREATE TABLE timeStamps (
        requestId integer REFERENCES requests,
        timeStamp integer NOT NULL,
        count integer NOT NULL,
        PRIMARY KEY (requestId, timestamp)
    );