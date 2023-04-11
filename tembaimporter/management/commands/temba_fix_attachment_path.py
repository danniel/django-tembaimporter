import logging

from django.conf import settings
from django.core.management.base import BaseCommand

from temba.msgs.models import Msg


logger = logging.getLogger("temba_client")
logger.setLevel(logging.INFO)


class Command(BaseCommand):
    help = (
        "Update the file storage URL for message attachment media."
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "current_host",
            type=str,
            help="The original S3 host (ie: rapidpro-courier.s3.us-east-1.amazonaws.com)",
        )
        parser.add_argument(
            "new_host",
            type=str,
            help="The new S3 host (ie: rapidpro-new.s3.eu-west-1.amazonaws.com)",
        )

    def handle(self, *args, **options) -> None:
        current_host = options.get("current_host", "")
        new_host = options.get("new_host", "")
        total = 0

        for msg in Msg.objects.filter(attachments__len__gt=0).all():
            new_attachments = []
            for attachment in msg.attachments:
                new_attachments.append(attachment.replace(current_host, new_host))
            msg.attachments = new_attachments
            msg.save()
            total += len(new_attachments)

        self.stdout.write(self.style.SUCCESS("Processed %d attachments." % total))
