from fastapi import APIRouter, HTTPException
from enum import Enum
from src import database as db
from src.datatypes import Character, Movie, Conversation, Line
from fastapi.params import Query
import sqlalchemy

router = APIRouter()


@router.get("/lines/{movie_id}/", tags=["lines"])
def get_lines(
    movie_id: int,
    character: str = "",
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    This endpoint returns all the lines in a movie as a list. Each entry contains:
    * `character` : character's name
    * `line` : the line
    
    The conversations can be filtered to only show lines with a certain character 
    using the `character` query parameter.
    """

    movie = db.movies.get(movie_id)
    if movie:

        filter_fn = lambda line: line.movie_id == movie_id
        if character:
            filter_fn = lambda line: line.movie_id == movie_id \
                        and db.characters.get(line.c_id).name == character.upper()

        lines = [l for l in filter(filter_fn, db.lines.values())]
        lines.sort(key=lambda l: (l.conv_id, l.line_sort))

        result = (
            {
                "character" : (lambda c: c and c.name)(db.characters.get(l.c_id)),
                "line" : l.line_text
            }
            for l in lines[offset : offset + limit]
        )
        return result

    raise HTTPException(status_code=404, detail="movie not found.")


@router.get("/conversations/{movie_id}/", tags=["lines"])
def get_conversations(
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

    movie = db.movies.get(movie_id)
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


@router.get("/conversation/{conversation_id}", tags=["lines"])
def get_conversation(conversation_id: int):
    """
    This endpoint gets details about a conversation:
    * `movie_id`: the internal id of the movie.
    * `title`: The title of the movie.
    * `characters`: list of the 2 characters in the conversation
    * `num_lines`: numebr of lines in the conversation
    * `lines`: list of lines in the conversation spoken by each character

    Each character is represented by a dictionary with the following keys:
    * `character_id`: the internal id of the character.
    * `name`: The name of the character.

    Lines are in the format "character name" : "line text"    

    """

    conv_stmt = (
        sqlalchemy.select(
            db.movies.c.id,
            db.movies.c.title,
            db.characters.c.name,

        )
    )
    s1 = sqlalchemy.text(
        "SELECT c.movie_id, m.title, c1.id, c1.name, c2.id, c2.name FROM conversations AS c " + \
        "LEFT JOIN movies AS m ON c.movie_id = m.id " + \
        "LEFT JOIN characters AS c1 ON c1.id = c.character1_id " + \
        "LEFT JOIN characters AS c2 ON c2.id = c.character2_id " + \
        "WHERE c.id = (:id)"
    )

    with db.engine.connect() as conn:
        conn.execute(s1, [{"id" : conversation_id}])

    
    conv = db.conversations.get(conversation_id)
    if conv:
        charname = lambda c: c and c.name
        lines = list(filter(lambda l: l.conv_id == conversation_id, db.lines.values()))
        lines.sort(key=lambda l: l.line_sort)
        result = {
            "movie_id" : conv.movie_id,
            "title" : (lambda m: m and m.title)(db.movies.get(conv.movie_id)),
            "characters" : (
                {
                    "character_id" : id,
                    "name" : charname(db.characters.get(id))
                }
                for id in [conv.c1_id, conv.c2_id]
            ),
            "num_lines" : conv.num_lines,
            "lines" : (
                {
                    "character" : charname(db.characters.get(l.c_id)),
                    "line" : l.line_text
                }
                for l in lines
            )
        }
        return result

    raise HTTPException(status_code=404, detail="movie not found.")
