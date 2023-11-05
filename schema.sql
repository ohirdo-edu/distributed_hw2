DROP TABLE IF EXISTS resumes_hobbies;
DROP TABLE IF EXISTS resumes;
DROP TABLE IF EXISTS hobbies;
DROP TABLE IF EXISTS users;

CREATE TABLE IF NOT EXISTS hobbies (
    id int PRIMARY KEY,
    name varchar
);

CREATE TABLE IF NOT EXISTS users (
    id int PRIMARY KEY,
    name varchar,
    password varchar
);

CREATE TABLE IF NOT EXISTS resumes (
    id int PRIMARY KEY,
    user_id serial REFERENCES users(id),
    city varchar,
    company varchar
);

CREATE TABLE IF NOT EXISTS resumes_hobbies (
    resume_id int REFERENCES resumes(id) ON UPDATE CASCADE ON DELETE CASCADE,
    hobby_id int REFERENCES hobbies(id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT bill_product_pkey PRIMARY KEY (resume_id, hobby_id)
);
