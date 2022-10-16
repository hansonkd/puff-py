__version__ = "0.1.0"

import puff
from puff import graphql, global_pubsub
from puff.contrib.django import query_and_params
from dataclasses import dataclass
from typing import Optional, Tuple, List, Any, Iterable
import django
from django.utils import timezone

django.setup()

from polls.models import Choice, Question


CHANNEL = "my_puff_chat_channel"
pubsub = global_pubsub()


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
    def choices(
        cls, context, /
    ) -> Tuple[List[ChoiceObject], str, List[Any], List[str], List[str]]:
        from polls.models import Choice

        # Extract column values from the previous layer to use in this one.
        parent_values = [r[0] for r in context.parent_values(["id"])]
        sql_q, params = puff.contrib.django.query_and_params(
            Choice.objects.filter(question_id__in=parent_values)
        )
        return ..., sql_q, params, ["id"], ["question_id"]


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
    def choices(
        cls, context, /
    ) -> Tuple[List[ChoiceObject], str, List[Any], List[str], List[str]]:
        # Extract column values from the previous layer to use in this one.
        parent_values = [r[0] for r in context.parent_values(["id"])]
        sql_q, params = puff.contrib.django.query_and_params(
            Choice.objects.filter(question_id__in=parent_values)
        )
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
        question = Question.objects.create(
            question_text=question_text, pub_date=timezone.now()
        )
        return question


@dataclass
class MessageObject:
    message_text: str
    from_connection_id: str
    num_processed: int


@dataclass
class Subscription:
    @classmethod
    def read_messages_from_channel(
        cls, context, /, connection_id: Optional[str] = None
    ) -> Iterable[MessageObject]:
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
                yield MessageObject(
                    message_text=msg.text,
                    from_connection_id=from_connection_id,
                    num_processed=num_processed,
                )
                num_processed += 1


@dataclass
class Schema:
    query: Query
    mutation: Mutation
    subscription: Subscription
