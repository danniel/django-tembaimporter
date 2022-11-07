import logging
import os
import time
from collections.abc import Iterable
from typing import Any

from django.contrib.auth.models import User
from django.core.management.base import BaseCommand
from django.db.models.query import QuerySet
from temba.api.v2 import serializers
from temba.archives.models import Archive
from temba.campaigns.models import Campaign, CampaignEvent
from temba.contacts.models import (Contact, ContactField, ContactGroup,
                                   ContactGroupCount, ContactURN, URN)
from temba.orgs.models import Org
from temba.msgs.models import Broadcast
from temba_client.v2 import TembaClient

logger = logging.getLogger("temba_client")
logger.setLevel(logging.DEBUG)


class Command(BaseCommand):
    help = (
        "Import Temba data from a remote API."
        "If at least one row already exists for a specific model it will skip its import."
    )

    @staticmethod
    def clean_api_url(url: str) -> str:
        """ Cleans up the API URL provided by the user """
        if not url:
            return ''
        return url.removesuffix('/').removesuffix('/api/v2').strip()

    @staticmethod
    def clean_api_key(key: str) -> str:
        """ Cleans up the API Key provided by the user """
        if not key:
            return ''
        return key.lower().removeprefix('token').strip()

    @staticmethod
    def inverse_choices(mapping: Iterable[tuple[str, Iterable]]) -> list[dict[str, str]]:
        """ Inverse lookup to find the CHOICES key from the provided value """
        result = {}
        for row in mapping:
            result[row[0]] = {v: k for k, v in row[1]}
        return result

    @property
    def default_fields(self) -> dict[str, Any]:
        return {
            'is_system': False,
            'org': self.default_org,
            'created_by': self.default_user,
            'modified_by': self.default_user,
        }

    def throttle(self) -> None:
        """ Pause the execution thread for a few seconds """
        if self.throttle_requests:
            SECONDS = 5
            self.stdout.write("Sleeping %d seconds..." % SECONDS)
            time.sleep(SECONDS)

    def __init__(self, *args, **kwargs):
        self.default_org = None
        self.default_user = None
        self.throttle_requests = False
        super().__init__(*args, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument(
            'api_url', type=str, 
            help='Remote API host (ie: http://rapidpro.ilhasoft.mobi)')
        parser.add_argument(
            'api_key', type=str, 
            help='Remote API key (ie: abcdef1234567890abcdef1234567890)')
        parser.add_argument(
            '--flush', action='store_true', 
            help="Delete existing records before importing the remote data")
        parser.add_argument(
            '--throttle', action='store_true', 
            help="Slow down the API interrogations by taking some pauses")

    def handle(self, *args, **options):
        api_url = Command.clean_api_url(
            options.get('api_url', os.environ.get('REMOTE_API_URL', '')))
        api_key = Command.clean_api_key(
            options.get('api_key', os.environ.get('REMOTE_API_KEY', '')))
        self.client = TembaClient(api_url, api_key)
        
        # Use the first admin user we can find in the destination database
        self.default_user = User.objects.filter(
            is_superuser=True, is_active=True).all()[0]  # type: User
        
        # Use the first organization we can find in the destination database
        self.default_org = Org.objects.filter(
            is_active=True, is_anon=False).all()[0]  # type: Org
        
        if options.get('throttle'):
            self.throttle_requests = True

        if options.get('flush'):
            self._flush_records()

        # Copy data from the remote API
        # The order in which we copy the data is important because of object relationships

        if ContactField.objects.count():
            self.write_notice('Skipping contact fields.')
        else:
            copy_result = self._copy_fields()
            self.write_success('Copied %d fields.' % copy_result)

        if ContactGroup.objects.count():
            self.write_notice('Skipping contact groups.')
        else:
            copy_result = self._copy_groups()
            self.write_success('Copied %d groups.' % copy_result)

        if Contact.objects.count():
            self.write_notice('Skipping contacts.')
        else:
            copy_result = self._copy_contacts()
            self.write_success('Copied %d contacts.' % copy_result)

        if Archive.objects.count():
            self.write_notice('Skipping archives.')
        else:
            copy_result = self._copy_archives()
            self.write_success('Copied %d archives.' % copy_result)

        if Campaign.objects.count():
            self.write_notice('Skipping campaigns.')
        else:
            copy_result = self._copy_campaigns()
            self.write_success('Copied %d campaigns.' % copy_result)

    def write_success(self, message: str):
        self.stdout.write(self.style.SUCCESS(message))

    def write_notice(self, message: str):
        self.stdout.write(self.style.NOTICE(message))

    def _flush_records(self) -> None:
        ContactURN.objects.all().delete()
        Contact.objects.all().delete()
        ContactGroupCount.objects.all().delete()
        ContactGroup.objects.all().delete()
        ContactField.objects.all().delete()

    def _copy_archives(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices(
            (("period", serializers.ArchiveReadSerializer.PERIODS.items()), ))
        
        for read_batch in self.client.get_archives().iterfetches(retry_on_rate_exceed=True):
            creation_queue = []
            for row in read_batch:
                # Older Temba versions use the "download_url" instead of "url"
                url = row.download_url if not hasattr(row, 'url') else row.url
                # Remove some common substrings in order to make the URL fit the 200 char limit
                url = url.replace("https://rapidpro-static-app.s3.amazonaws.com", "")
                url = url.replace("response-content-disposition=attachment%3B&", "")
                url = url.replace("response-content-type=application%2Foctet&", "")
                url = url.replace("response-content-encoding=none&", "")

                item_data = {
                    'org': self.default_org,
                    'archive_type': row.archive_type,
                    'start_date': row.start_date,
                    'period': inverse_choice['period'][row.period],
                    'record_count': row.record_count,
                    'size': row.size,
                    'hash': row.hash,
                    'url': url,
                    'build_time': 0,
                }
                item = Archive(**item_data)
                creation_queue.append(item)
            total += len(Archive.objects.bulk_create(creation_queue))
            self.throttle()
        return total            

    def _copy_fields(self) -> int:
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
            self.throttle()
        return total            

    def _copy_groups(self) -> int:
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
            self.throttle()
        return total            

    def _get_groups_uuid_pk(self) -> QuerySet:
        """ Retrieve all existing group uuids and their corresponding ids """
        return {item[0]: item[1] for item in ContactGroup.objects.values_list('uuid', 'pk')}

    def _copy_contacts(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices(
            (("status", serializers.ContactReadSerializer.STATUSES.items()), ))

        groups_uuid_pk = self._get_groups_uuid_pk()
        
        for read_batch in self.client.get_contacts().iterfetches(retry_on_rate_exceed=True):
            contact_group_uuids = {}
            contact_urns = {}
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

                item = Contact(**item_data)
                creation_queue.append(item)

                # current contact's URNs
                contact_urns[row.uuid] = row.urns

                # current contact's group memberships
                contact_group_uuids[row.uuid] = []
                for g in row.groups:
                    contact_group_uuids[row.uuid].append(g.uuid)

            contacts_created = Contact.objects.bulk_create(creation_queue)
            total += len(contacts_created)

            group_through_queue = []  # the m2m "through" objects
            contact_urns_queue = []  # the ContactURN objects
            for contact in contacts_created:
                for guuid in contact_group_uuids[contact.uuid]:
                    gid = groups_uuid_pk.get(guuid, None)
                    # Use the Django's "through" table and bulk add the contact_id + contactgroup_id pairs
                    group_through_queue.append(Contact.groups.through(contact_id=contact.id, contactgroup_id=gid))
                for urn in contact_urns[contact.uuid]:
                    urn_scheme, urn_path, urn_query, urn_display = URN.to_parts(urn)
                    contact_urns_queue.append(ContactURN(
                        org=self.default_org,
                        contact=contact,
                        scheme=urn_scheme,
                        path=urn_path,
                        identity=urn,
                        display=urn_display
                    ))
            Contact.groups.through.objects.bulk_create(group_through_queue)
            ContactURN.objects.bulk_create(contact_urns_queue)

            self.throttle()

        return total            

    def _copy_campaigns(self) -> int:
        total = 0
        groups_uuid_pk = self._get_groups_uuid_pk()
        for read_batch in self.client.get_campaigns().iterfetches(retry_on_rate_exceed=True):
            creation_queue = []
            for row in read_batch:
                item_data = {
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'modified_by': self.default_user,
                    'uuid': row.uuid,
                    'name': row.name,
                    'is_archived': row.archived,
                    'created_on': row.created_on,
                    'group_id': groups_uuid_pk[row.group.uuid] if row.group else None,
                }
                item = Campaign(**item_data)
                creation_queue.append(item)
            total += len(Campaign.objects.bulk_create(creation_queue))
        return total            
