from fastapi import APIRouter, HTTPException
from enum import Enum
from src import database as db
from src.datatypes import Character, Movie, Conversation, Line
from fastapi.params import Query
import sqlalchemy

router = APIRouter()


# include top 3 actors by number of lines
@router.get("/movies/{movie_id}", tags=["movies"])
def get_movie(movie_id: int):
    """
    This endpoint returns a single movie by its identifier. For each movie it returns:
    * `movie_id`: the internal id of the movie.
    * `title`: The title of the movie.
    * `top_characters`: A list of characters that are in the movie. The characters
      are ordered by the number of lines they have in the movie. The top five
      characters are listed.

    Each character is represented by a dictionary with the following keys:
    * `character_id`: the internal id of the character.
    * `character`: The name of the character.
    * `num_lines`: The number of lines the character has in the movie.

    """
    # if movie_id.isnumeric():
    #     movie_id = int(movie_id)
    

    
    movie = db.movies.get(movie_id)
    if movie:
        top_chars = [
            {
                "character_id" : c.id,
                "character" : c.name,
                "num_lines" : c.num_lines
            }
            for c in db.characters.values() if c.movie_id == movie_id
        ]
        top_chars.sort(key=lambda c: c["num_lines"], reverse=True)

        result = {
            "movie_id" : movie_id,
            "title" : movie.title,
            "top_characters" : top_chars[0:5]
        }
        return result

    raise HTTPException(status_code=404, detail="movie not found.")


class movie_sort_options(str, Enum):
    movie_title = "movie_title"
    year = "year"
    rating = "rating"


# Add get parameters
@router.get("/movies/", tags=["movies"])
def list_movies(
    name: str = "",
    limit: int = Query(50, ge=1, le=250),
    offset: int = Query(0, ge=0),
    sort: movie_sort_options = movie_sort_options.movie_title,
):
    """
    This endpoint returns a list of movies. For each movie it returns:
    * `movie_id`: the internal id of the movie. Can be used to query the
      `/movies/{movie_id}` endpoint.
    * `movie_title`: The title of the movie.
    * `year`: The year the movie was released.
    * `imdb_rating`: The IMDB rating of the movie.
    * `imdb_votes`: The number of IMDB votes for the movie.

    You can filter for movies whose titles contain a string by using the
    `name` query parameter.

    You can also sort the results by using the `sort` query parameter:
    * `movie_title` - Sort by movie title alphabetically.
    * `year` - Sort by year of release, earliest to latest.
    * `rating` - Sort by rating, highest to lowest.

    The `limit` and `offset` query
    parameters are used for pagination. The `limit` query parameter specifies the
    maximum number of results to return. The `offset` query parameter specifies the
    number of results to skip before returning results.
    """
    
    if sort is movie_sort_options.movie_title:
        order_by = db.movies.c.title
    elif sort is movie_sort_options.year:
        order_by = db.movies.c.year
    elif sort is movie_sort_options.rating:
        order_by = sqlalchemy.desc(db.movies.c.imdb_rating)
    else:
        raise HTTPException(400, "Invalid sort option")

    stmt = (
        sqlalchemy.select(
            db.movies.c.id,
            db.movies.c.title,
            db.movies.c.year,
            db.movies.c.imdb_rating,
            db.movies.c.imdb_votes,
        )
        .limit(limit)
        .offset(offset)
        .order_by(order_by, db.movies.c.id)
    )

    # filter only if name parameter is passed
    if name:
        stmt = stmt.where(db.movies.c.title.ilike(f"%{name}%"))

    with db.engine.connect() as conn:
        result = conn.execute(stmt)
        json = (
            {
                "movie_id" : row.id,
                "movie_title" : row.title,
                "year" : str(row.year),
                "imdb_rating" : row.imdb_rating,
                "imdb_votes" : row.imdb_votes
            }
            for row in result
        )

        return json
