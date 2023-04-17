from fastapi import APIRouter, HTTPException
from enum import Enum
from src import database as db
from src.datatypes import Character, Movie, Conversation, Line
from fastapi.params import Query

router = APIRouter()

# script search
@router.get("/lines/{movie_id}", tags=["lines"])
def get_lines9(movie_id: int):
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

@router.get("/conversations/{movie_id}/", tags=["lines"])
def get_lines(
    movie_id: int,
    character: str = "",
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    This endpoint returns all the conversations in a movie. Each conversation contains:
    * `conversation_id` : internal conversation id
    * `lines` : list of lines spoken by each character in the conversation. 
                The lines are in dictionaries in the following format:
        * `character` : character's name
        * `line` : the line
    
    The conversations can be filtered to only show lines with a certain character 
    using the `character` query parameter.
    """

    movie = db.conversations.get(movie_id)
    if movie:

        filter_fn = lambda line: line.movie_id == movie_id
        if character:
            filter_fn = lambda line: line.movie_id == movie_id \
                        and db.characters.get(line.c_id).name == character.upper()


        conv_lines = {}

        for line in filter(filter_fn, db.lines.values()):
            if line.conv_id not in conv_lines:
                conv_lines[line.conv_id] = []
            conv_lines[line.conv_id].append(line)
        
        for line_list in conv_lines.values():
            line_list.sort(key=lambda l: l.line_sort)

        convs = list(conv_lines.items())
        convs.sort() # sorts by first part of tuple, which is the id
        
        charname = lambda c: c and c.name

        result = (
            {
                "conversation_id" : id,
                "lines" : (
                    {
                        "character" : charname(db.characters.get(line.c_id)),
                        "line" : line.line_text
                    }
                    for line in line_list
                )
            }
            for id, line_list in convs[offset : offset + limit]
        )
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
    if name:
        filter_fn = lambda m: m.title and (name.lower() in m.title)
    else:
        filter_fn = lambda _: True
    items = list(filter(filter_fn, db.movies.values()))
    if sort == movie_sort_options.movie_title:
        items.sort(key=lambda m: m.title)
    elif sort == movie_sort_options.year:
        items.sort(key=lambda m: m.year)
    elif sort == movie_sort_options.rating:
        items.sort(key=lambda m: m.imdb_rating, reverse=True)

    json = (
        {
            "movie_id" : m.id,
            "movie_title" : m.title,
            "year" : m.year,
            "imdb_rating" : m.imdb_rating,
            "imdb_votes" : m.imdb_votes
        }
        for m in items[offset:offset+limit]
    )

    return json
