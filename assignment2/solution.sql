/* Author: Austin Derbique
    Course: CSE 511 Data Processing
    Term: Spring 2021
    Assignment #: 2
    Assignment Description: SQL Query for Movie Recommendation
    Assignment Notes: I could not fix the syntax issues on question 10.
    Assignment Citations: Various articles assisted in this assignment including the following: Wikipedia.org, Instructor given PDF Slides, Github.com, sqlservercentral.com. Although referencing other material, this work is my own and may not be copied. Refer to ASU academic policy for more information.
*/

/* Task 1:  Write a SQL query to return the total number of movies for each genre. Your query result should be saved in a table called “query1” which has two attributes: name, moviecount*/
CREATE TABLE query1 AS (
  SELECT
    Genre.name,
    count as moviecount
  FROM
    (
      SELECT
        count(HasAGenre.movieid),
        HasAGenre.genreid
      FROM
        hasagenre HasAGenre
      GROUP BY
        HasAGenre.genreid
    ) C,
    genres Genre
  WHERE
    C.genreid = Genre.genreid
);

/* Task 2: Write a SQL query to return the average rating per genre. Your query result should be saved in a table called “query2” which has two attributes: name, rating.*/
CREATE TABLE query2 AS (
  SELECT
    Genre.name,
    C.rating
  FROM
    (
      SELECT
        HasAGenre.genreid,
        AVG(Ratings.rating) as rating
      FROM
        hasagenre HasAGenre,
        ratings Ratings
      WHERE
        HasAGenre.movieid = Ratings.movieid
      GROUP BY
        genreid
    ) C,
    genres Genre
  WHERE
    Genre.genreid = C.genreid
);

/* Task 3: Write a SQL query to return the movies have at least 10 ratings. Your query result should be saved in a table called “query3” which has two attributes: title, CountOfRatings. */
CREATE TABLE query3 AS (
  SELECT
    Movies_1.title,
    C.CountOfRatings
  FROM
    (
      SELECT
        count(Ratings.rating) as CountOfRatings,
        Ratings.movieid
      FROM
        ratings Ratings
      GROUP BY
        Ratings.movieid
    ) C,
    movies Movies_1
  WHERE
    C.movieid = Movies_1.movieid
    AND C.CountOfRatings >= 10
);


/* Task 4: Write a SQL query to return all “Comedy” movies, including movieid and title. Your query result should be saved in a table called “query4” which has two attributes, movieid and title. */
CREATE TABLE query4 AS (
  SELECT
    movies.movieid,
    movies.title
  FROM
    movies
    INNER JOIN hasagenre ON movies.movieid = hasagenre.movieid
    INNER JOIN genres ON hasagenre.genreid = genres.genreid
  WHERE
    genres.name = 'Comedy'
);

/* Task 5: Write a SQL query to return the average rating per movie. Your query result should be saved in a table called “query5” which has two attributes, title and average. */
CREATE TABLE query5 AS (
  SELECT
    title,
    AVG(rating) AS rating
  FROM
    ratings,
    movies
  WHERE
    ratings.movieid = movies.movieid
  GROUP BY
    movies.movieid
);

/* Task 6: Write a SQL query to return the average rating for all “Comedy” movies. Your query result should be saved in a table called “query6” which has one attribute, average. */
CREATE TABLE query6 AS (
  SELECT
    AVG(Ratings_1.rating) AS rating
  FROM
    ratings Ratings_1
  WHERE
    Ratings_1.movieid IN (
      SELECT
        HasAGenre_1.movieid
      FROM
        hasagenre HasAGenre_1
      WHERE
        HasAGenre_1.genreid IN (
          SELECT
            Genres_1.genreid
          FROM
            genres Genres_1
          WHERE
            Genres_1.name = 'Comedy'
        )
    )
);

/* Task 7: Write a SQL query to return the average rating for all movies and each of these movies is both “Comedy” and “Romance”. Your query result should be saved in a table called “query7” which has one attribute, average. */
CREATE TABLE query7 AS (
  SELECT
    AVG(Ratings_1.rating) AS rating
  FROM
    ratings Ratings_1
  WHERE
    Ratings_1.movieid IN (
      SELECT
        HasAGenre_1.movieid
      FROM
        hasagenre HasAGenre_1
      WHERE
        HasAGenre_1.genreid = (
          SELECT
            Genres_1.genreid
          FROM
            genres Genres_1
          WHERE
            Genres_1.name = 'Romance'
        )
        AND (
          HasAGenre_1.genreid = (
            SELECT
              Genres_1.genreid
            FROM
              genres Genres_1
            WHERE
              Genres_1.name = "Comedy"
          )
        )
    )
);

/* Task 8:  Write a SQL query to return the average rating for all movies and each of these movies is “Romance” but not “Comedy”. Your query result should be saved in a table called “query8” which has one attribute, average. */
CREATE TABLE query8 AS
SELECT
  avg(ratings.rating) AS average
FROM
  movies,
  hasagenre,
  genres,
  ratings
WHERE
  movies.movieid = hasagenre.movieid
  AND hasagenre.genreid = genres.genreid
  AND ratings.movieid = movies.movieid
  AND genres.name = 'Romance'
  AND movies.movieid NOT IN (
    SELECT
      movies.movieid
    FROM
      movies,
      hasagenre,
      genres
    WHERE
      movies.movieid = hasagenre.movieid
      AND hasagenre.genreid = genres.genreid
      AND genres.name = 'Comedy'
  );

/* Task 9: Find all movies that are rated by a User such that the userId is equal to v1. The v1 will be an integer parameter passed to the SQL query. Your query result should be saved in a table called “query9” which has two attributes, movieid and rating. */
CREATE TABLE query9 AS
SELECT
  movieid,
  rating
from
  ratings
WHERE
  userid = :v1;

/* Task 10: Write a SQL query to create a recommendation model using item-based collaborative filtering. Given a userID v1(the same parameter used in q9), you need to recommend the movies according to the movies he has rated before. In particular, you need to predict the rating P of a movie i that the user Ua didn’t rate. In the following recommendation model, P(Ua, i) is the predicted rating of movie i for User Ua. L contains all movies that have been rated by Ua. Sim(i,l) is the sim_table between i and l. r is the rating that Ua gave to l. */
CREATE TABLE averages_table AS (
  SELECT
    Counts.avg,
    Movies_1.movieid
  FROM
    (
      SELECT
        AVG(Ratings_1.rating) AS avg,
        Ratings_1.movieid
      FROM
        ratings Ratings_1
      GROUP BY
        Ratings_1.movieid
    ) Counts,
    movies Movies_1
  WHERE
    Movies_1.movieid = Counts.movieid
);

CREATE TABLE rating_table AS (
  SELECT
    Ratings_1.movieid,
    Ratings_1.rating
  FROM
    ratings Ratings_1
  WHERE
    Ratings_1.userid = :v1
);

CREATE TABLE predictions AS (
  SELECT
    Similarities_1.movieid_a AS movieid,
    SUM(Similarities_1.sim * U.rating) / SUM(Similarities_1.sim) AS prediction
  FROM
    similarity Similarities_1,
    rating_table U
  WHERE
    Similarities_1.movieid_b = U.movieid
    AND Similarities_1.movieid_a NOT IN (
      SELECT
        U.movieid
      FROM
        rating_table U
    )
  GROUP BY
    Similarities_1.movieid_a
  ORDER BY
    prediction ASC
);

CREATE TABLE recommendation AS (
  SELECT
    Movies_1.title
  FROM
    movies Movies_1,
    predictions Predictions_1

  WHERE
    Predictions_1.prediction > 3.9
    AND
    Predictions_1.movieid = Movies_1.movieid
    
);