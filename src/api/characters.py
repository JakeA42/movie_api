from fastapi import APIRouter, HTTPException
from enum import Enum
from collections import Counter

from fastapi.params import Query
from src import database as db
import json
import sqlalchemy

router = APIRouter()

def get_top_conv_characters(character):
    c_id = character.id
    movie_id = character.movie_id
    all_convs = filter(
        lambda conv: conv.movie_id == movie_id and (conv.c1_id == c_id or conv.c2_id == c_id),
        db.conversations.values()
    )
    line_counts = Counter()

    for conv in all_convs:
        other_id = conv.c2_id if conv.c1_id == c_id else conv.c1_id
        line_counts[other_id] += conv.num_lines

    return line_counts.most_common()
    
@router.get("/characters/{id}", tags=["characters"])
def get_character(id: int):
    """
    This endpoint returns a single character by its identifier. For each character
    it returns:
    * `character_id`: the internal id of the character. Can be used to query the
      `/characters/{character_id}` endpoint.
    * `character`: The name of the character.
    * `movie`: The movie the character is from.
    * `gender`: The gender of the character.
    * `top_conversations`: A list of characters that the character has the most
      conversations with. The characters are listed in order of the number of
      lines together. These conversations are described below.

    Each conversation is represented by a dictionary with the following keys:
    * `character_id`: the internal id of the character.
    * `character`: The name of the character.
    * `gender`: The gender of the character.
    * `number_of_lines_together`: The number of lines the character has with the
      originally queried character.
    """

    char_stmt = (
        sqlalchemy.select(
            db.characters.c.id,
            db.characters.c.name,
            db.movies.c.title,
            db.characters.c.gender,
        )
        .join(db.movies, db.movies.c.id == db.characters.c.movie_id)
        .where(db.characters.id == id)
    )
    conv_stmt = sqlalchemy.text(
        """
        SELECT 
        """
    )

    if False:
        # print("character found")
        movie = db.movies.get(character.movie_id)
        result = {
            "character_id" : character.id,
            "character" : character.name,
            "movie" : movie and movie.title,
            "gender" : character.gender,
            "top_conversations" : (
                {
                    "character_id" : other_id,
                    "character" : db.characters[other_id].name,
                    "gender" : db.characters[other_id].gender,
                    "number_of_lines_together" : lines
                }
                for other_id, lines in get_top_conv_characters(character)
            )
        }
        return result

    raise HTTPException(status_code=404, detail="character not found.")


class character_sort_options(str, Enum):
    character = "character"
    movie = "movie"
    number_of_lines = "number_of_lines"


@router.get("/characters/", tags=["characters"])
def list_characters(
    name: str = "",
    limit: int = Query(50, ge=1, le=250),
    offset: int = Query(0, ge=0),
    sort: character_sort_options = character_sort_options.character,
):
    """
    This endpoint returns a list of characters. For each character it returns:
    * `character_id`: the internal id of the character. Can be used to query the
      `/characters/{character_id}` endpoint.
    * `character`: The name of the character.
    * `movie`: The movie the character is from.
    * `number_of_lines`: The number of lines the character has in the movie.

    You can filter for characters whose name contains a string by using the
    `name` query parameter.

    You can also sort the results by using the `sort` query parameter:
    * `character` - Sort by character name alphabetically.
    * `movie` - Sort by movie title alphabetically.
    * `number_of_lines` - Sort by number of lines, highest to lowest.

    The `limit` and `offset` query
    parameters are used for pagination. The `limit` query parameter specifies the
    maximum number of results to return. The `offset` query parameter specifies the
    number of results to skip before returning results.
    """
    
    if sort is character_sort_options.character:
        order_by = "c.name"
    elif sort is character_sort_options.movie:
        order_by = "m.title"
    elif sort is character_sort_options.number_of_lines:
        order_by = "cnl.num_lines DESC"
    else:
        raise HTTPException(400, "Invalid sort option")

    where = "WHERE c.name ilike (:cname)" if name else ""

    stmt = sqlalchemy.text(
        f"""
        WITH chars_num_lines AS (
            SELECT character_id, count(*) AS num_lines
            FROM lines
            GROUP BY character_id
        )
        SELECT c.id, c.name, m.title, cnl.num_lines FROM characters AS c
        LEFT JOIN chars_num_lines AS cnl ON cnl.character_id = c.id
        LEFT JOIN movies AS m ON c.movie_id = m.id
        {where}
        ORDER BY {order_by}, id
        LIMIT (:qlimit)
        OFFSET (:qoffset)
        """
    )

    with db.engine.connect() as conn:
        result = conn.execute(stmt, {"cname" : name, "qlimit" : limit, "qoffset" : offset})
        json = (
            {
                "character_id" : row.id,
                "character" : row.name,
                "movie" : row.title,
                "number_of_lines" : row.num_lines
            }
            for row in result
        )

        return json
