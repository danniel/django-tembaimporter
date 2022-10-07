import os

from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = 'Import data from a remote API'

    # def add_arguments(self, parser):
    #     parser.add_argument(
    #         'base_url', type=str, 
    #         help="Base URL for remote API (ie: https://rapidpro.ilhasoft.mobi/api/v2/")
    #     parser.add_argument(
    #         'key', type=str, 
    #         help="Remote API KEY")

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('That was a test'))