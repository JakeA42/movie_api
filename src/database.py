import csv
from src.datatypes import Character, Movie, Conversation, Line
import os
import io
from supabase import Client, create_client
import dotenv

from sqlalchemy import create_engine
import sqlalchemy

def database_connection_url():
    dotenv.load_dotenv()
    DB_USER: str = os.environ.get("POSTGRES_USER")
    DB_PASSWD = os.environ.get("POSTGRES_PASSWORD")
    DB_SERVER: str = os.environ.get("POSTGRES_SERVER")
    DB_PORT: str = os.environ.get("POSTGRES_PORT")
    DB_NAME: str = os.environ.get("POSTGRES_DB")
    return f"postgresql://{DB_USER}:{DB_PASSWD}@{DB_SERVER}:{DB_PORT}/{DB_NAME}"

engine = sqlalchemy.create_engine(database_connection_url())

metadata_obj = sqlalchemy.MetaData()

movies = sqlalchemy.Table("movies", metadata_obj, autoload_with=engine)
characters = sqlalchemy.Table("characters", metadata_obj, autoload_with=engine)
lines = sqlalchemy.Table("lines", metadata_obj, autoload_with=engine)
conversations = sqlalchemy.Table("conversations", metadata_obj, autoload_with=engine)

"""
movies = {}
characters = {}
lines = {}
conversations = {}

# DO NOT CHANGE THIS TO BE HARDCODED. ONLY PULL FROM ENVIRONMENT VARIABLES.
dotenv.load_dotenv()
supabase_api_key = os.environ.get("SUPABASE_API_KEY")
supabase_url = os.environ.get("SUPABASE_URL")

if supabase_api_key is None or supabase_url is None:
    raise Exception(
        "You must set the SUPABASE_API_KEY and SUPABASE_URL environment variables."
    )

supabase: Client = create_client(supabase_url, supabase_api_key)

sess = supabase.auth.get_session()

# TODO: Below is purely an example of reading and then writing a csv from supabase.
# You should delete this code for your working example.

# START PLACEHOLDER CODE

# Reading in the log file from the supabase bucket
log_csv = (
    supabase.storage.from_("movie-api")
    .download("movie_conversations_log.csv")
    .decode("utf-8")
)
conv_csv = (
    supabase.storage.from_("movie-api")
    .download("conversations.csv")
    .decode("utf-8")
)
lines_csv = (
    supabase.storage.from_("movie-api")
    .download("lines.csv")
    .decode("utf-8")
)
characters_csv = (
    supabase.storage.from_("movie-api")
    .download("characters.csv")
    .decode("utf-8")
)
movies_csv = (
    supabase.storage.from_("movie-api")
    .download("movies.csv")
    .decode("utf-8")
)

logs = []
for row in csv.DictReader(io.StringIO(log_csv), skipinitialspace=True):
    logs.append(row)


# Writing to the log file and uploading to the supabase bucket
def upload_new_log():
    output = io.StringIO()
    csv_writer = csv.DictWriter(
        output, fieldnames=["post_call_time", "movie_id_added_to"]
    )
    csv_writer.writeheader()
    csv_writer.writerows(logs)
    supabase.storage.from_("movie-api").upload(
        "movie_conversations_log.csv",
        bytes(output.getvalue(), "utf-8"),
        {"x-upsert": "true"},
    )


# END PLACEHOLDER CODE

conv_records = []
lines_records = []

def add_conv(new_conv: Conversation):
    conversations[new_conv.id] = new_conv
    conv_records.append({
        "conversation_id" : new_conv.id,
        "character1_id" : new_conv.c1_id,
        "character2_id" : new_conv.c2_id,
        "movie_id" : new_conv.movie_id
    })

def add_line(new_line: Line):
    lines[new_line.id] = new_line
    lines_records.append({
        "line_id" : new_line.id,
        "character_id" : new_line.c_id,
        "movie_id" : new_line.movie_id,
        "conversation_id" : new_line.conv_id,
        "line_sort" : new_line.line_sort,
        "line_text" : new_line.line_text
    })

def upload_convs():
    global conv_csv
    output = io.StringIO(conv_csv)
    output.seek(0, io.SEEK_END)
    csv_writer = csv.DictWriter(
        output, fieldnames=["conversation_id","character1_id","character2_id","movie_id"]
    )
    csv_writer.writerows(conv_records)
    conv_csv = output.getvalue()
    supabase.storage.from_("movie-api").upload(
        "conversations.csv",
        bytes(conv_csv, "utf-8"),
        {"x-upsert": "true"},
    )
    conv_records.clear()

def upload_lines():
    global lines_csv
    output = io.StringIO(lines_csv)
    output.seek(0, io.SEEK_END)
    csv_writer = csv.DictWriter(
        output, fieldnames=["line_id","character_id","movie_id","conversation_id","line_sort","line_text"]
    )
    csv_writer.writerows(lines_records)
    lines_csv = output.getvalue()
    supabase.storage.from_("movie-api").upload(
        "lines.csv",
        bytes(lines_csv, "utf-8"),
        {"x-upsert": "true"},
    )
    lines_records.clear()

print("reading movies")

def try_parse(type, val):
    try:
        return type(val)
    except ValueError:
        return None


#with open("movies.csv", mode="r", encoding="utf8") as csv_file:
movies = {
    try_parse(int, row["movie_id"]) :
    Movie(
        try_parse(int, row["movie_id"]),
        row["title"] or None,
        row["year"] or None,
        try_parse(float, row["imdb_rating"]),
        try_parse(int, row["imdb_votes"]),
        row["raw_script_url"] or None
    )
    for row in csv.DictReader(io.StringIO(movies_csv), skipinitialspace=True)
}

#with open("characters.csv", mode="r", encoding="utf8") as csv_file:
characters = {}
for row in csv.DictReader(io.StringIO(characters_csv), skipinitialspace=True):
    char = Character(
        try_parse(int, row["character_id"]),
        row["name"] or None,
        try_parse(int, row["movie_id"]),
        row["gender"] or None,
        try_parse(int, row["age"]),
        0
    )
    characters[char.id] = char

#with open("conversations.csv", mode="r", encoding="utf8") as csv_file:
conversations = {}
for row in csv.DictReader(io.StringIO(conv_csv), skipinitialspace=True):
    conv = Conversation(
        try_parse(int, row["conversation_id"]),
        try_parse(int, row["character1_id"]),
        try_parse(int, row["character2_id"]),
        try_parse(int, row["movie_id"]),
        0
    )
    conversations[conv.id] = conv

#with open("lines.csv", mode="r", encoding="utf8") as csv_file:
lines = {}
for row in csv.DictReader(io.StringIO(lines_csv), skipinitialspace=True):
    line = Line(
        try_parse(int, row["line_id"]),
        try_parse(int, row["character_id"]),
        try_parse(int, row["movie_id"]),
        try_parse(int, row["conversation_id"]),
        try_parse(int, row["line_sort"]),
        row["line_text"]
    )
    lines[line.id] = line
    c = characters.get(line.c_id)
    if c:
        c.num_lines += 1

    
    conv = conversations.get(line.conv_id)
    if conv:
        conv.num_lines += 1

"""
