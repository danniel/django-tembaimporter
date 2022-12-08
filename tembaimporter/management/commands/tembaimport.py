import logging
import os
import time
from collections.abc import Iterable
from functools import cache
from typing import Any, Dict, TypeVar

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Model
from temba.api.v2 import serializers
from temba.archives.models import Archive
from temba.campaigns.models import Campaign, CampaignEvent
from temba.channels.models import Channel, ChannelCount, ChannelEvent
from temba.contacts.models import (URN, Contact, ContactField, ContactGroup,
                                   ContactGroupCount, ContactURN)
from temba.flows.models import Flow, FlowRun, FlowStart
from temba.locations.models import AdminBoundary, BoundaryAlias
from temba.msgs.models import Broadcast, BroadcastMsgCount, Label, Msg
from temba.orgs.models import Org, User
from temba.tickets.models import Ticketer, Topic
from temba.tickets.types.internal import InternalType
from temba_client.v2 import TembaClient
from temba_client.v2 import types as client_types


UUID = TypeVar('UUID', bound=str)
ID = TypeVar('ID', bound=int)

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
        result: dict[str, str] = {}
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
            logger.info("Taking a %d second pause.", SECONDS)
            time.sleep(SECONDS)

    def __init__(self, *args, **kwargs):
        self.default_org = None
        self.default_user = None
        self.throttle_requests = False
        super().__init__(*args, **kwargs)

    def add_arguments(self, parser) -> None:
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

    def handle(self, *args, **options) -> None:
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
            self.write_notice('Deleting existing database records...')
            self._flush_records()
            self.write_success('Deleted existing database records.')

        # Copy data from the remote API
        # The order in which we copy the data is important because of object relationships

        if AdminBoundary.objects.count():
            self.write_notice('Skipping the administrative boundaries.')
        else:
            copy_result = self._copy_boundaries()
            self.write_success('Copied %d administrative boundaries.' % copy_result)

        self._update_default_org()
        self.write_success('Updated the default Org (Workspace).')

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

        if Channel.objects.count():
            self.write_notice('Skipping channels.')
        else:
            copy_result = self._copy_channels()
            self.write_success('Copied %d channels.' % copy_result)

        if Label.objects.count():
            self.write_notice('Skipping labels.')
        else:
            copy_result = self._copy_labels()
            self.write_success('Copied %d labels.' % copy_result)

        if Broadcast.objects.count():
            self.write_notice('Skipping broadcasts.')
        else:
            copy_result = self._copy_broadcasts()
            self.write_success('Copied %d broadcasts.' % copy_result)

        if Msg.objects.count():
            self.write_notice('Skipping messages.')
        else:
            copy_result = self._copy_messages()
            self.write_success('Copied %d messages.' % copy_result)

        if ChannelEvent.objects.count():
            self.write_notice('Skipping channel events.')
        else:
            copy_result = self._copy_channel_events()
            self.write_success('Copied %d channel events.' % copy_result)

        if Ticketer.objects.count():
            self.write_notice('Skipping ticketers.')
        else:
            copy_result = self._copy_ticketers()
            self.write_success('Copied %d ticketers.' % copy_result)

        if Topic.objects.count():
            self.write_notice('Skipping topics.')
        else:
            copy_result = self._copy_topics()
            self.write_success('Copied %d topics.' % copy_result)

        if User.objects.count() > 3:
            # Skip if we have more than the default admin user and the AnonymousUser
            # TODO: set the check for > 3 because I can't delete my test user right now
            self.write_notice('Skipping users.')
        else:
            copy_result = self._copy_users()
            self.write_success('Copied %d users.' % copy_result)

        if Flow.objects.count():
            self.write_notice('Skipping flows.')
        else:
            copy_result = self._copy_flows()
            self.write_success('Copied %d flows.' % copy_result)


    def write_success(self, message: str) -> None:
        self.stdout.write(self.style.SUCCESS(message))

    def write_notice(self, message: str) -> None:
        self.stdout.write(self.style.NOTICE(message))

    def _flush_records(self) -> None:
        """
        Delete most of the existing database records before importing them
        again from the remote host though the API
        """
        Flow.objects.all().delete()
        logger.info("Deleted flows.")

        # Delete users except the AnonymousUser and the default admin user
        if self.default_user:
            User.objects.exclude(
                pk=self.default_user.pk
            ).exclude(
                username=settings.ANONYMOUS_USER_NAME
            ).exclude(
                username="test1@example.com"  # TODO: For now do not delete my test user
            ).all().delete()
        else:
            User.objects.all().delete()
        logger.info("Deleted users.")
        
        # Delete administrative boundaries starting with the lowest administrative level
        BoundaryAlias.objects.all().delete()
        AdminBoundary.objects.filter(level=3).delete()
        AdminBoundary.objects.filter(level=2).delete()
        AdminBoundary.objects.filter(level=1).delete()
        AdminBoundary.objects.all().delete()
        logger.info("Deleted boundaries.")

        Topic.objects.all().delete()
        logger.info("Deleted topics.")

        Ticketer.objects.all().delete()
        logger.info("Deleted ticketers.")

        ChannelEvent.objects.all().delete()
        logger.info("Deleted channel events.")

        Msg.objects.all().delete()
        logger.info("Deleted messages.")

        BroadcastMsgCount.objects.all().delete()
        Broadcast.objects.all().delete()
        logger.info("Deleted broadcasts.")

        Label.objects.all().delete()
        logger.info("Deleted labels.")

        ChannelCount.objects.all().delete()
        Channel.objects.all().delete()
        logger.info("Deleted channels.")
        
        Campaign.objects.all().delete()
        logger.info("Deleted campaigns.")

        Archive.objects.all().delete()
        logger.info("Deleted archives.")

        ContactURN.objects.all().delete()
        logger.info("Deleted contact URNs.")

        Contact.objects.all().delete()
        logger.info("Deleted contacts.")

        ContactGroupCount.objects.all().delete()
        ContactGroup.objects.all().delete()
        logger.info("Deleted contact groups.")

        ContactField.objects.all().delete()
        logger.info("Deleted contact fields.")

    @property
    @cache
    def _get_groups_uuid_pk(self) -> Dict[UUID, ID]:
        """ Retrieve all existing Group uuids and their corresponding database id """
        return {item[0]: item[1] for item in ContactGroup.objects.values_list('uuid', 'pk')}

    @property
    @cache
    def _get_contacts_uuid_pk(self) -> Dict[UUID, ID]:
        """ Retrieve all existing Contact uuids and their corresponding database id """
        return {item[0]: item[1] for item in Contact.objects.values_list('uuid', 'pk')}

    @property
    @cache
    def _get_urns_pk(self) -> Dict[UUID, ID]:
        """ Retrieve all existing URNs and their corresponding database id """
        return {item[0]: item[1] for item in ContactURN.objects.values_list('identity', 'pk')}

    @property
    @cache
    def _get_channels_uuid_pk(self) -> Dict[UUID, ID]:
        """ Retrieve all existing Channel uuids and their corresponding database id """
        return {item[0]: item[1] for item in Channel.objects.values_list('uuid', 'pk')}

    @property
    @cache
    def _get_labels_uuid_pk(self) -> Dict[UUID, ID]:
        """ Retrieve all existing Label uuids and their corresponding database id """
        return {item[0]: item[1] for item in Label.objects.values_list('uuid', 'pk')}

    @property
    @cache
    def _get_flows_uuid_pk(self) -> Dict[UUID, ID]:
        """ Retrieve all existing Flow uuids and their corresponding database id """
        return {item[0]: item[1] for item in Flow.objects.values_list('uuid', 'pk')}


    def _update_default_org(self):
        org_data = self.client.get_org()

        # Get the Org country by boundary name
        try:
            country = AdminBoundary.objects.filter(name=org_data.country)[0]
        except IndexError:
            # Get the Org country by boundary alias name
            try:
                country = BoundaryAlias.objects.filter(name=org_data.country)[0]
            except IndexError:
                self.default_org.country = None
            else:
                self.default_org.country = country
        else:
            self.default_org.country = country

        self.default_org.uuid = org_data.uuid
        self.default_org.name = org_data.name
        self.default_org.languages = org_data.languages
        self.default_org.primary_language = org_data.primary_language
        self.default_org.timezone = org_data.timezone
        self.default_org.date_style = org_data.date_style
        self.default_org.credits = org_data.credits
        self.default_org.is_anon = org_data.anon
        self.default_org.save()

    def _copy_archives(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices(
            (("period", serializers.ArchiveReadSerializer.PERIODS.items()), ))
        
        for read_batch in self.client.get_archives().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Archive] = []
            row: client_types.Archive
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
            logger.info("Total archives bulk created: %d.", total)
            self.throttle()
        return total            

    def _copy_fields(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices(
            (("value_type", serializers.ContactFieldReadSerializer.VALUE_TYPES.items()), ))
        
        for read_batch in self.client.get_fields().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[ContactField] = []
            row: client_types.Field
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
            logger.info("Total contact fields bulk created: %d.", total)
            self.throttle()
        return total            

    def _copy_groups(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices(
            (("status", serializers.ContactGroupReadSerializer.STATUSES.items()), ))
        
        ContactGroup.create_system_groups(self.default_org)

        for read_batch in self.client.get_groups().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[ContactGroup] = []
            row: client_types.Group
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
            logger.info("Total groups bulk created: %d.", total)
            self.throttle()
        return total            

    def _copy_contacts(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices(
            (("status", serializers.ContactReadSerializer.STATUSES.items()), ))

        groups_uuid_pk = self._get_groups_uuid_pk
        
        for read_batch in self.client.get_contacts().iterfetches(retry_on_rate_exceed=True):
            contact_group_uuids: dict[UUID, list[UUID]] = {}
            contact_urns: dict[UUID, list[str]] = {}
            creation_queue: list[Contact] = []
            row: client_types.Contact
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
            logger.info("Total contacts bulk created: %d.", total)

            group_through_queue: list[Model] = []  # the m2m "through" objects
            contact_urns_queue: list[ContactURN] = []  # the ContactURN objects
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
            logger.info("Added groups and URNs to the created contacts.")
            self.throttle()
        return total            

    def _copy_campaigns(self) -> int:
        total = 0
        groups_uuid_pk = self._get_groups_uuid_pk
        for read_batch in self.client.get_campaigns().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Campaign] = []
            row: client_types.Campaign
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
            logger.info("Total campaigns bulk created: %d.", total)
            self.throttle()
        return total            

    def _copy_channels(self) -> int:
        total = 0
        for read_batch in self.client.get_channels().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Channel] = []
            row: client_types.Channel
            for row in read_batch:
                item_data = {
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'modified_by': self.default_user,
                    'uuid': row.uuid,
                    'name': row.name,
                    'created_on': row.created_on,
                    'last_seen': row.last_seen,
                    'address': row.address,
                    'country': row.country,
                    'device': row.device,
                }
                # TODO: channel_type?
                item = Channel(**item_data)
                creation_queue.append(item)
            total += len(Channel.objects.bulk_create(creation_queue))
            logger.info("Total channels bulk created: %d.", total)
            self.throttle()
        return total            

    def _copy_channel_events(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices(
            (("event_type", serializers.ChannelEventReadSerializer.TYPES.items()), ))

        channels_uuid_pk = self._get_channels_uuid_pk
        contacts_uuid_pk = self._get_contacts_uuid_pk        

        for read_batch in self.client.get_channel_events().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[ChannelEvent] = []
            row: client_types.ChannelEvent
            for row in read_batch:
                # Skip channel events for channels which don't seem to exist anymore
                if row.channel.uuid not in channels_uuid_pk:
                    logger.warning("Skipping channel events for channel %s %s", row.channel.uuid, row.channel.name)
                    continue
                item_data = {
                    'org': self.default_org,
                    'id': row.id,
                    'event_type': inverse_choice['event_type'][row.type],
                    'contact_id': contacts_uuid_pk.get(row.contact.uuid, None) if row.contact else None,
                    'channel_id': channels_uuid_pk[row.channel.uuid] if row.channel else None,
                    'extra': row.extra,
                    'occurred_on': row.occurred_on,
                    'created_on': row.created_on,
                }
                item = ChannelEvent(**item_data)
                creation_queue.append(item)
            total += len(ChannelEvent.objects.bulk_create(creation_queue))
            logger.info("Total channel events bulk created: %d.", total)
            self.throttle()
        return total            

    def _copy_labels(self) -> int:
        total = 0
        for read_batch in self.client.get_labels().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Label] = []
            row: client_types.Label
            for row in read_batch:
                item_data = {
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'modified_by': self.default_user,
                    'uuid': row.uuid,
                    'name': row.name,
                }
                item = Label(**item_data)
                creation_queue.append(item)
            total += len(Label.objects.bulk_create(creation_queue))
            logger.info("Total labels bulk created: %d.", total)
            self.throttle()
        return total            

    def _copy_broadcasts(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices(
            (("status", serializers.BroadcastReadSerializer.STATUSES.items()), ))
        
        # This could use a lot of memory
        groups_uuid_pk = self._get_groups_uuid_pk
        contacts_uuid_pk = self._get_contacts_uuid_pk
        urns_pk = self._get_urns_pk

        for read_batch in self.client.get_broadcasts().iterfetches(retry_on_rate_exceed=True):
            contact_group_uuids: dict[ID, list[UUID]] = {}
            contact_urns: dict[ID, list[str]] = {}
            contact_uuids: dict[ID, list[UUID]] = {}
            creation_queue: list[Broadcast] = []

            row: client_types.Broadcast
            for row in read_batch:
                item_data = {
                    'id': row.id,
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'created_on': row.created_on,
                    'status': inverse_choice['status'][row.status],
                    'text': row.text,
                }
                item = Broadcast(**item_data)
                creation_queue.append(item)

                contact_urns[row.id] = row.urns
                contact_group_uuids[row.id] = []
                for g in row.groups:
                    contact_group_uuids[row.id].append(g.uuid)
                contact_uuids[row.id] = []
                for c in row.contacts:
                    contact_uuids[row.id].append(c.uuid)

            broadcasts_created = Broadcast.objects.bulk_create(creation_queue)
            total += len(broadcasts_created)
            logger.info("Total broadcasts bulk created: %d.", total)

            # the m2m "through" objects
            group_through_queue: list[Model] = []  
            contact_through_queue: list[Model] = []
            urn_through_queue: list[Model] = []
            
            for broadcast in broadcasts_created:
                for guuid in contact_group_uuids[broadcast.id]:
                    gid = groups_uuid_pk.get(guuid, None)
                    group_through_queue.append(
                        Broadcast.groups.through(broadcast_id=broadcast.id, contactgroup_id=gid))
                for cuuid in contact_uuids[broadcast.id]:
                    cid = contacts_uuid_pk.get(cuuid, None)
                    contact_through_queue.append(
                        Broadcast.contacts.through(broadcast_id=broadcast.id, contact_id=cid))
                for urn in contact_urns[broadcast.id]:
                    uid = urns_pk.get(urn, None)
                    urn_through_queue.append(
                        Broadcast.urns.through(broadcast_id=broadcast.id, urn_id=uid))

            Broadcast.groups.through.objects.bulk_create(group_through_queue)
            Broadcast.contacts.through.objects.bulk_create(contact_through_queue)
            Broadcast.urns.through.objects.bulk_create(urn_through_queue)
            logger.info("Added groups, contacts, and URNs to created broadcasts.")
            self.throttle()
        return total            

    def _copy_messages(self) -> int:
        total = 0
        contacts_uuid_pk = self._get_contacts_uuid_pk
        channels_uuid_pk = self._get_channels_uuid_pk
        labels_uuid_pk = self._get_labels_uuid_pk
        urns_pk = self._get_urns_pk

        inverse_choice = Command.inverse_choices((
            ("direction", [(Msg.DIRECTION_IN, "in"), (Msg.DIRECTION_OUT, "out")]), 
            ("type", serializers.MsgReadSerializer.TYPES.items()), 
            ("status", serializers.MsgReadSerializer.STATUSES.items()), 
            ("visibility", serializers.MsgReadSerializer.VISIBILITIES.items()), 
        ))
        
        for read_batch in self.client.get_messages().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Msg] = []
            label_uuids: dict[ID, list[UUID]] = {}

            row: client_types.Message
            for row in read_batch:
                item_data = {
                    'org': self.default_org,
                    'id': row.id,
                    'broadcast_id': row.broadcast,
                    'direction': inverse_choice['direction'][row.direction],
                    'msg_type': inverse_choice['type'][row.type],
                    'status': inverse_choice['status'][row.status],
                    'visibility': inverse_choice['visibility'][row.visibility],

                    'contact_id': contacts_uuid_pk.get(row.contact.uuid, None) if row.contact else None,
                    'contact_urn_id': urns_pk.get(row.urn, None) if row.urn else None,
                    'channel_id': channels_uuid_pk.get(row.channel.uuid, None) if row.channel else None,
                    'attachments': row.attachments,

                    'created_on': row.created_on,
                    'sent_on': row.sent_on,
                    'modified_on': row.modified_on,
                    'text': row.text,
                }
                
                item = Msg(**item_data)
                creation_queue.append(item)
                
                label_uuids[row.id] = []
                for label in row.labels:
                    label_uuids[row.id].append(label.uuid)

            msgs_created = Msg.objects.bulk_create(creation_queue)
            total += len(msgs_created)
            logger.info("Total messages bulk created: %d.", total)

            label_through_queue: list[Model] = []
            for msg in msgs_created:
                for luuid in label_uuids[msg.id]:
                    lid = labels_uuid_pk.get(luuid, None)
                    label_through_queue.append(
                        Msg.labels.through(msg_id=msg.id, label_id=lid))
            Msg.labels.through.objects.bulk_create(label_through_queue)
            logger.info("Added labels to created messages.")
            self.throttle()
        return total            

    def _copy_ticketers(self) -> int:
        total = 0
        for read_batch in self.client.get_ticketers().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Ticketer] = []
            row: client_types.Ticketer
            for row in read_batch:
                item_data = {
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'modified_by': self.default_user,
                    'uuid': row.uuid,
                    'name': row.name,
                    'created_on': row.created_on,
                    'ticketer_type': row.type,
                    'config': {},
                    'is_system': True if row.type == InternalType.slug else False,
                }
                item = Ticketer(**item_data)
                creation_queue.append(item)
            total += len(Ticketer.objects.bulk_create(creation_queue))
            logger.info("Total ticketers bulk created: %d.", total)
            self.throttle()
        return total            

    def _copy_topics(self) -> int:
        total = 0
        for read_batch in self.client.get_topics().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Topic] = []
            row: client_types.Topic
            for row in read_batch:
                item_data = {
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'modified_by': self.default_user,
                    'uuid': row.uuid,
                    'name': row.name,
                    'created_on': row.created_on,
                    'is_system': True if row.name == Topic.DEFAULT_TOPIC else False,
                    'is_default': True if row.name == Topic.DEFAULT_TOPIC else False,
                }
                item = Topic(**item_data)
                creation_queue.append(item)
            total += len(Topic.objects.bulk_create(creation_queue))
            logger.info("Total topics bulk created: %d.", total)
            self.throttle()
        return total            

    def _copy_users(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices(
            (("role", serializers.UserReadSerializer.ROLES.items()), ))

        for read_batch in self.client.get_users().iterfetches(retry_on_rate_exceed=True):
            row: client_types.User
            for row in read_batch:
                item_data = {
                    'username': row.email,
                    'email': row.email,
                    'first_name': row.first_name,
                    'last_name': row.last_name,
                    'date_joined': row.created_on,
                }
                org_role = inverse_choice['role'][row.role]
                item = User(**item_data)
                # Save users one by one instead of doing it in batches
                item.save()
                self.default_org.add_user(item, org_role)
                total += 1
            logger.info("Total users created: %d.", total)
            self.throttle()
        return total            

    def _copy_boundaries(self) -> int:
        total = 0
        osm_id_to_pk: dict[int, ID] = {}  # Map osm_id fields to primary keys
        osm_id_to_path: dict[int, str] = {}  # Map osm_id fields to paths
        for level in range(0, 4):
            for read_batch in self.client.get_boundaries().iterfetches(retry_on_rate_exceed=True):
                creation_queue: list[AdminBoundary] = []
                boundary_aliases: dict[int, list[str]] = {}  # Map osm_id fields to a list of alias names
                row: client_types.Boundary
                for row in read_batch:
                    # ignore boundaries on different levels than the current one
                    if row.level != level:
                        continue

                    if row.parent:
                        parent_path = osm_id_to_path.get(row.parent.osm_id, "")
                        item_path = parent_path + AdminBoundary.PADDED_PATH_SEPARATOR + row.name
                    else:
                        item_path = row.name
                    osm_id_to_path[row.osm_id] = item_path

                    item_data = {
                        'osm_id': row.osm_id,
                        'name': row.name,
                        'parent_id': osm_id_to_pk.get(row.parent.osm_id, None) if row.parent else None,
                        'path': item_path,
                        # 'simplified_geometry': row.geometry,  # We do not use the geometry
                        'level': row.level,
                        'lft': 0,
                        'rght': 0,
                        'tree_id': 0,
                    }
                    item = AdminBoundary(**item_data)
                    creation_queue.append(item)
                    boundary_aliases[row.osm_id] = []
                    boundary_aliases[row.osm_id].extend(row.aliases)
                
                with transaction.atomic():
                    # with AdminBoundary.objects.disable_mptt_updates():
                    boundaries_created = AdminBoundary.objects.bulk_create(creation_queue)
                    total += len(boundaries_created)
                    # AdminBoundary.objects.rebuild()  # TODO: Patch a TreeManager and rebuild the tree
                logger.info("Total boundaries bulk created: %d.", total)

                aliases_creation_queue: list[BoundaryAlias] = []
                for boundary in boundaries_created:
                    osm_id_to_pk[boundary.osm_id] = boundary.id
                    alias_names = boundary_aliases.get(boundary.osm_id, [])
                    for alias_name in alias_names:
                        aliases_creation_queue.append(BoundaryAlias(
                            name=alias_name, 
                            boundary_id=boundary.id,
                            org=self.default_org,
                            created_by=self.default_user,
                            modified_by=self.default_user,
                        ))
                BoundaryAlias.objects.bulk_create(aliases_creation_queue)
                logger.info("Added aliases to created boundaries.")
                self.throttle()
        return total            

    def _copy_flows(self) -> int:
        inverse_choice = Command.inverse_choices((
            ("type", serializers.FlowReadSerializer.FLOW_TYPES.items()), 
        ))
        total = 0
        for read_batch in self.client.get_flows().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Flow] = []
            row: client_types.Flow
            for row in read_batch:
                item_data = {
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'modified_by': self.default_user,
                    'uuid': row.uuid,
                    'name': row.name,
                    'created_on': row.created_on,
                    'modified_on': row.modified_on,
                    'is_archived': row.archived,
                    'expires_after_minutes': row.expires,
                    'runs': row.runs,
                    'flow_type': inverse_choice[row.type],
                    'metadata': {Flow.METADATA_RESULTS: row.results},
                }
                # TODO: parent_refs
                item = Flow(**item_data)
                creation_queue.append(item)
            total += len(Flow.objects.bulk_create(creation_queue))
            logger.info("Total flows bulk created: %d.", total)
            self.throttle()
        return total            

    def _copy_flow_starts(self) -> int:
        inverse_choice = Command.inverse_choices((
            ("status", serializers.FlowStartReadSerializer.STATUSES.items()), 
        ))
        flows_uuid_pk = self._get_flows_uuid_pk
        groups_uuid_pk = self._get_groups_uuid_pk
        contacts_uuid_pk = self._get_contacts_uuid_pk

        total = 0
        for read_batch in self.client.get_flow_starts().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[FlowStart] = []
            group_uuids: dict[ID, list[UUID]] = {}
            contact_uuids: dict[ID, list[UUID]] = {}
            row: client_types.FlowStart
            for row in read_batch:
                item_data = {
                    'org': self.default_org,
                    'created_by': self.default_user,
                    'uuid': row.uuid,
                    'created_on': row.created_on,
                    'modified_on': row.modified_on,
                    'flow': flows_uuid_pk.get(row.flow.uuid, None),
                    'status': inverse_choice[row.status],
                    'restart_participants': row.restart_participants,
                    'exclude_active': row.exclude_active,
                    'extra': row.extra,
                    'params': row.params,
                }

                item = FlowStart(**item_data)
                creation_queue.append(item)

                group_uuids[row.id] = []
                for group in row.groups:
                    group_uuids[row.id].append(group.uuid)

                contact_uuids[row.id] = []
                for contact in row.contacts:
                    contact_uuids[row.id].append(contact.uuid)

            flow_starts_created = FlowStart.objects.bulk_create(creation_queue)
            total += len(flow_starts_created)
            logger.info("Total flow starts bulk created: %d.", total)

            group_through_queue: list[Model] = []
            contact_through_queue: list[Model] = []
            for flow_start in flow_starts_created:
                for guuid in group_uuids[flow_start.id]:
                    gid = groups_uuid_pk.get(guuid, None)
                    group_through_queue.append(
                        FlowStart.groups.through(flow_start_id=flow_start.id, group_id=gid))
                for cuuid in contact_uuids[flow_start.id]:
                    cid = contacts_uuid_pk.get(cuuid, None)
                    contact_through_queue.append(
                        FlowStart.contacts.through(flow_start_id=flow_start.id, contact_id=cid))
            FlowStart.contacts.through.objects.bulk_create(contact_through_queue)
            logger.info("Added contacts to created flow starts.")
            FlowStart.groups.through.objects.bulk_create(group_through_queue)
            logger.info("Added groups to created flow starts.")

            self.throttle()
        return total            
