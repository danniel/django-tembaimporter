import os
import logging
from typing import Callable

from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth.models import User
from temba_client.v2 import TembaClient
from temba.api.v2 import serializers
from temba.orgs.models import Org
from temba.contacts.models import ContactGroup, ContactField


logger = logging.getLogger('temba_client')
logger.setLevel(logging.DEBUG)


class Command(BaseCommand):
    help = 'Import Temba data from a remote API'

    def add_arguments(self, parser):
        parser.add_argument(
            'api_url', type=str, 
            help='Remote API host (ie: http://rapidpro.ilhasoft.mobi)')
        parser.add_argument(
            'api_key', type=str, 
            help='Remote API key (ie: abcdef1234567890abcdef1234567890)')

    @staticmethod
    def clean_api_url(url):
        if not url:
            return ''
        return url.removesuffix('/').removesuffix('/api/v2').strip()

    @staticmethod
    def clean_api_key(key):
        if not key:
            return ''
        return key.lower().removeprefix('token').strip()

    def handle(self, *args, **options):
        api_url = Command.clean_api_url(
            options.get('api_url', os.environ.get('REMOTE_API_URL', '')))
        api_key = Command.clean_api_key(
            options.get('api_key', os.environ.get('REMOTE_API_KEY', '')))
        self.client = TembaClient(api_url, api_key)
        
        # Use the first admin user we can find in the destination database
        self.default_user = User.objects.filter(is_superuser=True, is_active=True).all()[0]
        
        # Use the first organization we can find in the destination database
        self.default_org = Org.objects.filter(is_active=True, is_anon=False).all()[0]

        # Copy data from the remote API
        total_fields = self._copy_fields()
        self.stdout.write(self.style.SUCCESS('Copied "%d" fields.\n' % total_fields))
        total_groups = self._copy_groups()
        self.stdout.write(self.style.SUCCESS('Copied "%d" groups.\n' % total_groups))

    def _copy_fields(self):
        total = 0
        # Inverse lookup to find the CHOICES key from the provided value
        inverse_choice = {}
        inverse_choice['value_type'] = {
            v: k for k, v in serializers.ContactFieldReadSerializer.VALUE_TYPES.items()}
        
        for read_batch in self.client.get_fields().iterfetches(retry_on_rate_exceed=True):
            creation_queue = []
            for row in read_batch:          
                item_data = {
                    'key': row.key,
                    'name': row.label,
                    'value_type': inverse_choice['value_type'][row.value_type],
                    'show_in_table': row.pinned,
                    'is_system': False,
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'modified_by': self.default_user,
                }
                item = ContactField(**item_data)
                creation_queue.append(item)
            total += len(ContactField.objects.bulk_create(creation_queue))
        return total            

    def _copy_groups(self):
        total = 0
        # Inverse lookup to find the CHOICES key from the provided value
        inverse_choice = {}
        inverse_choice['status'] = {
            v: k for k, v in serializers.ContactGroupReadSerializer.STATUSES.items()}
        
        for read_batch in self.client.get_groups().iterfetches(retry_on_rate_exceed=True):
            creation_queue = []
            for row in read_batch:
                # print("API =", row.__dict__)          
                item_data = {
                    'uuid': row.uuid,
                    'name': row.name,
                    'query': row.query,
                    'status': inverse_choice['status'][row.status],
                    'is_system': False,
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'modified_by': self.default_user,
                }
                # print("ITEM DATA =", item_data)
                item = ContactGroup(**item_data)
                creation_queue.append(item)
            total += len(ContactGroup.objects.bulk_create(creation_queue))
        return total            

