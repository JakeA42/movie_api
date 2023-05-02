from fastapi.exceptions import HTTPException
import sqlalchemy
from src.datatypes import Character, Conversation, Line
from fastapi import APIRouter
from src import database as db
from pydantic import BaseModel
from typing import List
from datetime import datetime


# FastAPI is inferring what the request body should look like
# based on the following two classes.
class LinesJson(BaseModel):
    character_id: int
    line_text: str


class ConversationJson(BaseModel):
    character_1_id: int
    character_2_id: int
    lines: List[LinesJson]


router = APIRouter()


@router.post("/movies/{movie_id}/conversations/", tags=["movies"],)
def add_conversation(movie_id: int, conversation: ConversationJson):
    """
    This endpoint adds a conversation to a movie. The conversation is represented
    by the two characters involved in the conversation and a series of lines between
    those characters in the movie.

    The endpoint ensures that all characters are part of the referenced movie,
    that the characters are not the same, and that the lines of a conversation
    match the characters involved in the conversation.

    Line sort is set based on the order in which the lines are provided in the
    request body.

    The endpoint returns the id of the resulting conversation that was created.
    """

    c1id = conversation.character_1_id
    c2id = conversation.character_2_id
    if c1id == c2id or c1id is None or c2id is None:
        raise HTTPException(status_code=400, detail="Invalid character(s)")

    for line in conversation.lines:
        if line.character_id not in [c1id, c2id]:
            raise HTTPException(status_code=400, detail="Incorrect character id in line list")
    with db.engine.begin() as conn:
        try:
            conv_stmt = (
                sqlalchemy.insert(db.conversations)
                .values(character1_id=c1id, character2_id=c2id, movie_id=movie_id)
                .returning(db.conversations.c.id)
            )
            conv_result = conn.execute(conv_stmt)
            conv_id = conv_result.scalar_one()
            lines_stmt = (
                sqlalchemy.insert(db.lines)
                .values([
                    {
                        "character_id" : line.character_id,
                        "movie_id" : movie_id,
                        "conversation_id": conv_id,
                        "line_sort" : i,
                        "line_text" : line.line_text
                    }
                    for i, line in enumerate(conversation.lines)
                ])
            )
            result = conn.execute(lines_stmt)
            return conv_id
        except Exception as e:
            raise HTTPException(422, str(e))


    # Validate input
    if movie_id not in db.movies:
        raise HTTPException(status_code=404, detail="Movie not found.")
    c1id = conversation.character_1_id
    c2id = conversation.character_2_id
    c1 = db.characters.get(c1id)
    c2 = db.characters.get(c2id)
    if c1id == c2id or c1 is None or c2 is None:
        raise HTTPException(status_code=400, detail="Invalid character(s)")
    if c1.movie_id != movie_id or c2.movie_id != movie_id:
        raise HTTPException(status_code=400, detail="Character(s) not part of the specified movie")

    
    # create a new conversation
    conv_id = max(db.conversations) + 1
    conv = Conversation(conv_id, c1id, c2id, movie_id, len(conversation.lines))
    db.conversations[conv_id] = conv
    db.conv_records.append({
        "conversation_id" : conv_id,
        "character1_id" : c1id,
        "character2_id" : c2id,
        "movie_id" : movie_id
    })

    line_id = max(db.lines) + 1
    for i, line in enumerate(conversation.lines):
        l = Line(line_id, line.character_id, movie_id, conv_id, i, line.line_text)
        db.add_line(l)
        line_id += 1
    
    db.upload_convs()
    db.upload_lines()



    # TODO: Remove the following two lines. This is just a placeholder to show
    # how you could implement persistent storage.

    print(conversation)
    db.logs.append({"post_call_time": datetime.now(), "movie_id_added_to": movie_id})
    db.upload_new_log()
