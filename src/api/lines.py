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

    stmt = (
        sqlalchemy.select(
            db.characters.c.name,
            db.lines.c.line_text
        )
        .join(db.characters, db.lines.c.character_id == db.characters.c.id)
        .where(db.lines.c.movie_id == movie_id)
        .order_by(db.lines.c.conversation_id, db.lines.c.line_sort)
        .limit(limit)
        .offset(offset)
    )
    if character:
        stmt = stmt.where(db.characters.c.name == character.upper())
    
    with db.engine.connect() as conn:
        result = conn.execute(stmt)
        json = (
            {
                "character" : row.name,
                "line" : row.line_text
            }
            for row in result
        )
        return json 
        

def split_convs(lines_list: sqlalchemy.engine.cursor.CursorResult):
    if lines_list.rowcount < 1:
        return

    conv_lines = []
    conv = None
    for row in lines_list:
        if not conv:
            conv = row.conversation_id
        if conv != row.conversation_id:
            yield conv, conv_lines
            conv_lines.clear()
            conv = row.conversation_id
        
        conv_lines.append({
            "character" : row.name,
            "line" : row.line_text
        })
    yield conv, conv_lines


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

    where = "WHERE c.name ilike (:cname)" if character else ""
    stmt = sqlalchemy.text(
        f"""
        WITH conv_lim_off AS (
            SELECT id FROM conversations
            WHERE movie_id = (:qmovie_id)
            ORDER BY id
            LIMIT (:qlimit)
            OFFSET (:qoffset)
        )
        SELECT l.conversation_id, c.name, l.line_sort, l.line_text
        FROM lines as l
        RIGHT JOIN conv_lim_off ON l.conversation_id = conv_lim_off.id
        LEFT JOIN characters AS c ON l.character_id = c.id
        {where}
        """
    )
    with db.engine.connect() as conn:
        result = conn.execute(stmt, {"qmovie_id" : movie_id, "cname" : character, "qlimit" : limit, "qoffset" : offset})
        json = (
            {
                "conversation_id" : conv,
                "lines" : conv_lines
            }
            for conv, conv_lines in split_convs(result)
        )

        return json


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
