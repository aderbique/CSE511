/* Author: Austin Derbique
    Course: CSE 511 Data Processing
    Term: Spring 2021
    Assignment #: 1
    Assignment Description: Create Movie Recommendation Database 
*/

/* Create users table */
CREATE TABLE users (
    userid INT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL
);

/* Create movies table */
CREATE TABLE movies (
    movieid INT NOT NULL PRIMARY KEY,
    title TEXT NOT NULL
);

/* Create taginfo table */
CREATE TABLE taginfo(
    tagid INT NOT NULL PRIMARY KEY,
    content TEXT NOT NULL
);

/* Create genres table */
CREATE TABLE genres(
    genreid INT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL
);

/* Create ratings table */
CREATE TABLE ratings(
    userid INT NOT NULL REFERENCES users(userid),
    movieid INT NOT NULL REFERENCES movies(movieid),
    PRIMARY KEY(userid, movieid),
    rating NUMERIC NOT NULL,
    timestamp BIGINT NOT NULL
);

/* Create tags table */
CREATE TABLE tags(
    userid INT NOT NULL REFERENCES users(userid),
    movieid INT NOT NULL REFERENCES movies(movieid),
    tagid INT NOT NULL REFERENCES taginfo(tagid),
    PRIMARY KEY(userid, movieid, tagid),
    timestamp BIGINT NOT NULL
);

/* Create hasagenre table */
CREATE TABLE hasagenre(
    movieid INT NOT NULL REFERENCES movies(movieid),
    genreid INT NOT NULL REFERENCES genres(genreid),
    PRIMARY KEY(movieid, genreid)
);