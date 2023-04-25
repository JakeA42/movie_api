from fastapi.testclient import TestClient

from src.api.server import app

import json

client = TestClient(app)


def count_testentries(response):
    count = 0
    for conv in response.json():
        if conv["lines"] == [
            {"character" : "BRUCE", "line" : "TEST1"},
            {"character" : "JOEY", "line" : "TEST2"},
            {"character" : "BRUCE", "line" : "TEST3"},
            {"character" : "JOEY", "line" : "TEST4"} ]:
            count += 1
    return count


def test_add_conversation1():
    req = """{
    "character_1_id": 1,
    "character_2_id": 4,
    "lines": [
        {
        "character_id": 1,
        "line_text": "TEST1"
        },
        {
        "character_id": 4,
        "line_text": "TEST2"
        },
        {
        "character_id": 1,
        "line_text": "TEST3"
        },
        {
        "character_id": 4,
        "line_text": "TEST4"
        }
    ]
    }"""
    response = client.get("/conversations/0/?limit=500&offset=100")
    assert response.status_code == 200

    pre = count_testentries(response)

    response = client.post("/movies/0/conversations/", content=req)
    assert response.status_code == 200

    response = client.get("/conversations/0/?limit=500&offset=100")
    assert response.status_code == 200

    assert count_testentries(response) == pre + 1


def test_add_conversation2():
    req = """{
    "character_1_id": 1,
    "character_2_id": 555,
    "lines": [
        {
        "character_id": 1,
        "line_text": "TEST1"
        },
        {
        "character_id": 2,
        "line_text": "TEST2"
        },
        {
        "character_id": 1,
        "line_text": "TEST3"
        },
        {
        "character_id": 2,
        "line_text": "TEST4"
        }
    ]
    }"""
    response = client.get("/conversations/0/?limit=500&offset=100")
    assert response.status_code == 200

    pre = count_testentries(response)

    response = client.post("/movies/0/conversations/", content=req)
    assert response.status_code == 400

    response = client.get("/conversations/0/?limit=500&offset=100")
    assert response.status_code == 200

    assert count_testentries(response) == pre

