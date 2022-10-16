import strawberry
from strawberry.schema.config import StrawberryConfig
from typing import List
from strawberry import auto
from strawberry_django_plus.optimizer import DjangoOptimizerExtension

from polls.models import Question, Choice


@strawberry.django.type(Choice)
class ChoiceType:
    id: auto
    question_id: auto
    choice_text: auto
    votes: int


@strawberry.django.type(Question)
class QuestionType:
    id: auto
    question_text: auto
    pub_date: auto
    choices: List[ChoiceType]


@strawberry.type
class Query:
    questions: List[QuestionType] = strawberry.django.field()


schema = strawberry.Schema(
    query=Query,
    config=StrawberryConfig(auto_camel_case=False),
    extensions=[
        # other extensions...
        DjangoOptimizerExtension,
    ],
)
