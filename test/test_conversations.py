from fastapi.testclient import TestClient

from src.api.server import app

import json

client = TestClient(app)


def test_add_conversation1():
    req = """{
    "character_1_id": 99991,
    "character_2_id": 99992,
    "lines": [
        {
        "character_id": 99991,
        "line_text": "TEST1"
        },
        {
        "character_id": 99992,
        "line_text": "TEST2"
        },
        {
        "character_id": 99991,
        "line_text": "TEST3"
        },
        {
        "character_id": 99992,
        "line_text": "TEST4"
        }
    ]
    }"""
    response = client.get("/conversations/99999/?limit=500&offset=100")
    assert response.status_code == 200

    response = client.post("/movies/99999/conversations/", content=req)
    assert response.status_code == 200

    response = client.get("/conversations/99999/?limit=500&offset=100")
    assert response.status_code == 200



def test_add_conversation2():
    # Test mismatched character ids
    req = """{
    "character_1_id": 99991,
    "character_2_id": 555,
    "lines": [
        {
        "character_id": 99991,
        "line_text": "TEST1"
        },
        {
        "character_id": 99992,
        "line_text": "TEST2"
        },
        {
        "character_id": 99991,
        "line_text": "TEST3"
        },
        {
        "character_id": 99992,
        "line_text": "TEST4"
        }
    ]
    }"""
    response = client.get("/conversations/0/?limit=500&offset=100")
    assert response.status_code == 200


    response = client.post("/movies/0/conversations/", content=req)
    assert response.status_code == 400

    response = client.get("/conversations/0/?limit=500&offset=100")
    assert response.status_code == 200


