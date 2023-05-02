from fastapi import APIRouter, HTTPException
from src import database as db
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


def split_convs(lines_list):
    for x in lines_list:
        pass


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

    stmt = (
        sqlalchemy.select(
            db.lines.c.conversation_id,
            db.characters.c.name,
            db.lines.c.line_sort,
            db.lines.c.line_text
        )
        .join(db.characters, db.characters.c.id == db.lines.c.character_id)
        .where(db.lines.c.movie_id == movie_id)
    )


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

    stmt_conv = sqlalchemy.text(
        """
        SELECT cv.movie_id, m.title, c1.id AS c1_id, c1.name AS c1_name, c2.id AS c2_id, c2.name AS c2_name
        FROM conversations AS cv
        LEFT JOIN movies AS m ON cv.movie_id = m.id
        LEFT JOIN characters AS c1 ON c1.id = cv.character1_id
        LEFT JOIN characters AS c2 ON c2.id = cv.character2_id
        WHERE cv.id = (:id)
        """
    )
    stmt_lines = sqlalchemy.text(
        """
        SELECT c.name, l.line_text FROM lines AS l
        LEFT JOIN characters AS c ON l.character_id = c.id
        WHERE l.conversation_id = (:id)
        ORDER BY l.line_sort
        """
    )

    with db.engine.connect() as conn:
        conv_result = conn.execute(stmt_conv, [{"id" : conversation_id}])
        lines_result = conn.execute(stmt_lines, [{"id" : conversation_id}])
        if conv_result.rowcount < 1:
            raise HTTPException(status_code=404, detail="conversation not found.")
        conv_row = conv_result.one()
        lines = (
            { row.name : row.line_text }
            for row in lines_result
        )
        json = {
            "movie_id" : conv_row.movie_id,
            "title" : conv_row.title,
            "characters" : [
                {"character_id" : conv_row.c1_id, "name" : conv_row.c1_name},
                {"character_id" : conv_row.c2_id, "name" : conv_row.c2_name}
            ],
            "num_lines" : lines_result.rowcount,
            "lines" : lines
        }
        return json
