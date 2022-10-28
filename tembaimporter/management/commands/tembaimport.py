import os
import logging
from typing import Callable

from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth.models import User
from temba_client.v2 import TembaClient
from temba.api.v2 import serializers
from temba.orgs.models import Org
from temba.contacts.models import ContactGroup, ContactField, Contact, ContactGroupCount
from temba.campaigns.models import Campaign
from temba.archives.models import Archive


logger = logging.getLogger('temba_client')
logger.setLevel(logging.DEBUG)


class Command(BaseCommand):
    help = 'Import Temba data from a remote API'

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

    @staticmethod
    def inverse_choices(mapping):
        """ Inverse lookup to find the CHOICES key from the provided value """
        result = {}
        for row in mapping:
            result[row[0]] = {v: k for k, v in row[1]}
        return result

    @property
    def default_fields(self):
        return {
            'is_system': False,
            'org': self.default_org,
            'created_by': self.default_user,
            'modified_by': self.default_user,
        }

    def __init__(self, *args, **kwargs):
        self.default_org = None
        self.default_user = None
        super().__init__(*args, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument(
            'api_url', type=str, 
            help='Remote API host (ie: http://rapidpro.ilhasoft.mobi)')
        parser.add_argument(
            'api_key', type=str, 
            help='Remote API key (ie: abcdef1234567890abcdef1234567890)')
        parser.add_argument('--flush', action='store_true', help="Flush existing records")

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

        if options.get('flush'):
            self._flush_records()

        # Copy data from the remote API
        # The order in which we copy the data is important because of object relationships

        copy_result = self._copy_fields()
        self.stdout.write(self.style.SUCCESS('Copied %d fields.\n' % copy_result))

        copy_result = self._copy_groups()
        self.stdout.write(self.style.SUCCESS('Copied %d groups.\n' % copy_result))

        copy_result = self._copy_contacts()
        self.stdout.write(self.style.SUCCESS('Copied %d contacts.\n' % copy_result))

        copy_result = self._copy_archives()
        self.stdout.write(self.style.SUCCESS('Copied %d archives.\n' % copy_result))

        copy_result = self._copy_campaigns()
        self.stdout.write(self.style.SUCCESS('Copied %d campaigns.\n' % copy_result))

    def _flush_records(self):
        ContactField.objects.all().delete()
        Contact.objects.all().delete()
        ContactGroupCount.objects.all().delete()
        ContactGroup.objects.all().delete()

    def _copy_archives(self):
        total = 0
        inverse_choice = Command.inverse_choices(
            (("period", serializers.ArchiveReadSerializer.PERIODS.items()), ))
        
        for read_batch in self.client.get_fields().iterfetches(retry_on_rate_exceed=True):
            creation_queue = []
            for row in read_batch:          
                item_data = {
                    'org': self.default_org,
                    'archive_type': row.archive_type,
                    'start_date': row.start_date,
                    'period': inverse_choice['period'][row.period],
                    'record_count': row.record_count,
                    'size': row.size,
                    'hash': row.hash,
                    'download_url': row.download_url,
                }
                item = Archive(**item_data)
                creation_queue.append(item)
            total += len(Archive.objects.bulk_create(creation_queue))
        return total            

    def _copy_fields(self):
        total = 0
        inverse_choice = Command.inverse_choices(
            (("value_type", serializers.ContactFieldReadSerializer.VALUE_TYPES.items()), ))
        
        for read_batch in self.client.get_fields().iterfetches(retry_on_rate_exceed=True):
            creation_queue = []
            for row in read_batch:          
                item_data = {
                    **self.default_fields,
                    'key': row.key,
                    'name': row.label,
                    'value_type': inverse_choice['value_type'][row.value_type],
                    'show_in_table': row.pinned,
                }
                item = ContactField(**item_data)
                creation_queue.append(item)
            total += len(ContactField.objects.bulk_create(creation_queue))
        return total            

    def _copy_groups(self):
        total = 0
        inverse_choice = Command.inverse_choices(
            (("status", serializers.ContactGroupReadSerializer.STATUSES.items()), ))
        
        ContactGroup.create_system_groups(self.default_org)

        for read_batch in self.client.get_groups().iterfetches(retry_on_rate_exceed=True):
            creation_queue = []
            for row in read_batch:
                item_data = {
                    **self.default_fields,
                    'uuid': row.uuid,
                    'name': row.name,
                    'query': row.query,
                    'status': inverse_choice['status'][row.status],
                    # TODO:
                    # The API doesn't give us the group type so we assume they're all 'Manual'
                    'group_type': ContactGroup.TYPE_MANUAL,
                }
                item = ContactGroup(**item_data)
                creation_queue.append(item)
            total += len(ContactGroup.objects.bulk_create(creation_queue))
        return total            

    def _get_groups_uuid_id(self):
        # Retrieve all existing group uuids and their corresponding ids
        return ContactGroup.objects.all().values_list('uuid', 'id', named=True)

    def _copy_contacts(self):
        total = 0
        inverse_choice = Command.inverse_choices(
            (("status", serializers.ContactReadSerializer.STATUSES.items()), ))

        groups_uuid_id = self._get_groups_uuid_id()
        
        for read_batch in self.client.get_contacts().iterfetches(retry_on_rate_exceed=True):
            contact_group_uuids = {}
            creation_queue = []
            for row in read_batch:
                item_data = {
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'modified_by': self.default_user,
                    'uuid': row.uuid,
                    'name': row.name,
                    'language': row.language,
                    'fields': row.fields,
                    'created_on': row.created_on,
                    'modified_on': row.modified_on,
                    'last_seen_on': row.last_seen_on,
                }
                if not hasattr(row, 'status') or row.status is None:
                    # The remote API is a Temba install older than v7.3.58 which doesn't have a status field
                    item_data |= {
                        'status': Contact.STATUS_BLOCKED if row.blocked else Contact.STATUS_STOPPED if row.stopped else Contact.STATUS_ACTIVE}
                else:
                    # The remote API is newer Temba install
                    item_data |= {'status': inverse_choice['status'][row.status] if row.status else None}

                #TODO: URNs are censored. Will resume work on this when the source Org will not hide phone numbers
                item = Contact(**item_data)
                creation_queue.append(item)

                contact_group_uuids[row.uuid] = []
                for g in row.groups:
                    contact_group_uuids[row.uuid].append(g.uuid)

            contacts_created = Contact.objects.bulk_create(creation_queue)
            total += len(contacts_created)

            # Add the m2m groups for each created contact
            group_through_queue = []
            for contact in contacts_created:
                for guuid in contact_group_uuids[contact.uuid]:
                    gid = groups_uuid_id.get(uuid=guuid)
                    # Use the Django's "through" table and bulk add the contact_id + group_id pairs
                    group_through_queue.append(Contact.groups.through(contact_id=contact.id, contactgroup_id=gid))
            Contact.groups.through.objects.bulk_create(group_through_queue)

        return total            

    def _copy_campaigns(self):
        total = 0
        for read_batch in self.client.get_campaigns().iterfetches(retry_on_rate_exceed=True):
            creation_queue = []
            for row in read_batch:
                item_data = {
                    'uuid': row.uuid,
                    'name': row.name,
                    'archived': row.archived,
                    'created_on': row.created_on,
                    'group__pk': row.group['uuid'],
                }
                item = Campaign(**item_data)
                creation_queue.append(item)
            total += len(Campaign.objects.bulk_create(creation_queue))
        return total            
