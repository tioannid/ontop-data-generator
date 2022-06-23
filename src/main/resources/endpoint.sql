-- endpoint database schema

-- drop database if it exists
DROP DATABASE IF EXISTS endpoint;

-- create database
CREATE DATABASE endpoint;

-- connect to the database
\connect endpoint

-- enable temporal extentions
-- NOTE: PostgreSQL installation must have been extended prior to this
--       https://github.com/jeff-davis/PostgreSQL-Temporal
CREATE EXTENSION temporal;

-- create tables
CREATE TABLE event (
id serial,
duration period,
description varchar(100),
time_propagated timestamp with time zone
);

-- select id, name, location, duration, creation_date from meeting
CREATE TABLE meeting (
id serial,
creation_date timestamp with time zone,
name varchar(30),
location varchar(50),
duration period,
description varchar(100));

-- create temporal indexes
CREATE INDEX event_duration_idx ON event USING GiST (duration);

CREATE INDEX meeting_duration_idx ON meeting USING GiST (duration);

-- run a test query to verify if temporal functionality is enabled
-- should return :
--               ?column? 
--               ----------
--               t
--               (1 row)
SELECT '[2009-01-01, 2009-03-01)'::period @> '[2009-02-03, 2009-02-07)'::period;
