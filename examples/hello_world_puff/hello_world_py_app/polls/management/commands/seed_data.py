from django.core.management.base import BaseCommand, CommandError
from polls.models import Question, Choice
from django.utils import timezone


class Command(BaseCommand):
    def handle(self, *args, **options):
        for x in range(1000):
            q = Question.objects.create(
                question_text=f"hello {x}", pub_date=timezone.now()
            )
            for y in range(10):
                c = Choice.objects.create(question=q, choice_text=f"hello choice {y}")
