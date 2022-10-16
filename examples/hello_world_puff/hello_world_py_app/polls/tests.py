from django.test import TestCase
from .models import Question, Choice
from puff import global_graphql
import pytest
from django.utils import timezone
from django.db import connection

gql = global_graphql()


# Create your tests here.
@pytest.mark.django_db
def test_polls():
    question = Question.objects.create(question_text="hi", pub_date=timezone.now())
    choice = Choice.objects.create(choice_text="choice 1", question=question)

    QUERY = """
    query {
        questions {
            question_text
            choices {
                choice_text
                votes
            }
        }
    }
    """

    result = gql.query(QUERY, {}, connection.connection.postgres_client)
    assert 'data' in result
    assert 'errors' not in result
    assert result['data']["questions"][0]["question_text"] == "hi"
    assert result['data']["questions"][0]["choices"][0]["choice_text"] == 'choice 1'

