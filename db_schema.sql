drop table if exists places CASCADE;

create table places
(
    id   serial
        constraint places_pk
            primary key,
    city varchar(255),
    county varchar(255),
    country varchar(255)
);

drop table if exists people CASCADE;

create table people
(
    -- id   serial
    --     constraint people_pk
    --         primary key,
    f_name varchar(255),
    l_name varchar(255),
    date_of_birth date,
    city_of_birth_id INTEGER REFERENCES places (id),
    PRIMARY KEY (f_name, l_name, date_of_birth)
);
