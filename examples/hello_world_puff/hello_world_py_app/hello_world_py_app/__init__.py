__version__ = '0.1.0'

import puff
from puff import graphql
from puff import global_pubsub
from dataclasses import dataclass
from typing import Optional, Tuple, List, Any, Iterable
import django
django.setup()

# pubsub = puff.global_pubsub()
CHANNEL = "my_puff_chat_channel"
pubsub = global_pubsub()

@dataclass
class SomeInputObject:
    some_count: int
    some_string: str


@dataclass
class DbObject:
    was_input: int
    title: str


@dataclass
class SomeObject:
    field1: int
    field2: str

    @classmethod
    def child_sub_query(cls, context, /) -> Tuple[DbObject, str, List[Any], List[str], List[str]]:
        # Extract column values from the previous layer to use in this one.
        parent_values = [r[0] for r in context.parent_values(["field1"])]
        sql_q = "SELECT a::int as was_input, $2 as title FROM unnest($1::int[]) a"
        # returning a sql query along with 2 lists allow you to correlate and join the parent with the child.
        return ..., sql_q, [parent_values, "from child"], ["field1"], ["was_input"]





@dataclass
class ChoiceObject:
    id: int
    question_id: int
    choice_text: str
    votes: int

from puff.contrib.django import query_and_params

@dataclass
class QuestionObject:
    id: int
    pub_date: str
    question_text: str

    @classmethod
    def choices(cls, context, /) -> Tuple[List[ChoiceObject], str, List[Any], List[str], List[str]]:
        from polls.models import Choice
        # Extract column values from the previous layer to use in this one.
        parent_values = [r[0] for r in context.parent_values(["id"])]
        sql_q, params = puff.contrib.django.query_and_params(Choice.objects.filter(question_id__in=parent_values))
        return ..., sql_q, params, ["id"], ["question_id"]

from polls.models import Choice, Question
from django.utils import timezone

@dataclass
class ChoiceObject:
    id: int
    question_id: int
    choice_text: str
    votes: int


@dataclass
class QuestionObject:
    id: int
    pub_date: str
    question_text: str

    @classmethod
    def choices(cls, context, /) -> Tuple[List[ChoiceObject], str, List[Any], List[str], List[str]]:
        # Extract column values from the previous layer to use in this one.
        parent_values = [r[0] for r in context.parent_values(["id"])]
        sql_q, params = puff.contrib.django.query_and_params(Choice.objects.filter(question_id__in=parent_values))
        return ..., sql_q, params, ["id"], ["question_id"]


@dataclass
class Query:

    @classmethod
    @graphql.borrow_db_context
    def questions(cls, context, /) -> Tuple[List[QuestionObject], str, List[Any]]:
        # Convert a Django queryset to sql and params to pass off to Puff. This function does 0 IO in Python.
        sql_q, params = query_and_params(Question.objects.all())
        return ..., sql_q, params

    @classmethod
    @graphql.borrow_db_context  # Decorate with borrow_db_context to use same DB connection in Django as the rest of GQL
    def question_objs(cls, context, /) -> Tuple[List[QuestionObject], List[Any]]:
        # You can also compute the python values with Django and hand them off to Puff.
        # This version of the same `questions` field, is slower since Django is constructing the objects.
        objs = list(Question.objects.all())
        return ..., objs

@dataclass
class Mutation:
    @classmethod
    @graphql.borrow_db_context  # Decorate with borrow_db_context to use same DB connection in Django as the rest of GQL
    def create_question(cls, context, /, question_text: str) -> QuestionObject:
        question = Question.objects.create(question_text=question_text, pub_date=timezone.now())
        return question


@dataclass
class MessageObject:
    message_text: str
    from_connection_id: str
    num_processed: int


@dataclass
class Subscription:
    @classmethod
    def read_messages_from_channel(cls, context, /, connection_id: Optional[str] = None) -> Iterable[MessageObject]:
        if connection_id is not None:
            conn = pubsub.connection_with_id(connection_id)
        else:
            conn = pubsub.connection()
        conn.subscribe(CHANNEL)
        num_processed = 0
        while msg := conn.receive():
            from_connection_id = msg.from_connection_id
            # Filter out messages from yourself.
            if connection_id != from_connection_id:
                yield MessageObject(message_text=msg.text, from_connection_id=from_connection_id, num_processed=num_processed)
                num_processed += 1


@dataclass
class Schema:
    query: Query
    mutation: Mutation
    subscription: Subscription
