import logging
import os
import requests
import time
from typing import Union, List
from collections.abc import Iterable
from collections import namedtuple
from functools import cache
from typing import Any, Dict, TypeVar

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Model
from temba.api.v2 import serializers
from temba.archives.models import Archive
from temba.channels.models import Channel, ChannelEvent
from temba.contacts.models import (
    URN,
    Contact,
    ContactField,
    ContactGroup,
    ContactURN,
)
from temba.flows.models import (
    Flow,
    FlowCategoryCount,
    FlowRun,
    FlowRunCount,
    FlowStart,
)
from temba.msgs.models import Broadcast, Label, Msg
from temba.orgs.models import Org, User
from temba_client.v2 import TembaClient
from temba_client.v2 import types as client_types


UUID = TypeVar("UUID", bound=str)
ID = TypeVar("ID", bound=int)

logger = logging.getLogger("temba_client")
logger.setLevel(logging.INFO)

CacheItem = namedtuple("CacheItem", "pk uuid old_uuid")


class WebSession():
    """
    A web session for sending regular web requests for data which
    is not published by the API
    """

    def __init__(self, host_url: str, user: str, password: str) -> None:
        if host_url.startswith("http://") or host_url.startswith("https://"):
            self.host = host_url
        else:
            self.host = "https://" + host_url

        self.user = user
        self.password = password
        self.session = requests.Session()

    def get(self, path: str) -> requests.models.Response:
        return self.session.get(self.host + path)

    def post(self, path: str, data: dict) -> requests.models.Response:
        full_url = self.host + path
        self.session.headers.update({"referer": full_url})
        if not data:
            data = {}
        return self.session.post(full_url, data=data)

    def login(self) -> requests.models.Response:
        self.get("/users/login/")
        result = self.post("/users/login/", data={
            "csrfmiddlewaretoken": self.session.cookies.get("csrftoken",""), 
            "username": self.user, 
            "password": self.password,
        })
        if result.status_code > 299 or result.status_code < 200:
            logger.error("Web login failed!")
            exit()
        return result

    @staticmethod
    def create_web_session(api_url: str, admin_user: str, admin_pass: str) -> "WebSession":
        ws = WebSession(api_url, admin_user, admin_pass)
        ws.login()
        return ws


def parse_broken_json(input: Union[str, None]) -> Union[str, dict, List[str], List[dict], None]:
    """
    Transforms inputs like:
        [{name: UReporters, uuid: 7ed6f520-1412-4af3-b9b4-f4886be7a05a}, {name: some, name, uuid: 123123123}]
    into Python objects like:
        [
            {"name": "Ureporters", "uuid": "7ed6f520-1412-4af3-b9b4-f4886be7a05a"},
            {"name": "some, name", "uuid": "123123123"},
        ]
    """

    def group_to_object(group_text: str) -> Union[str, dict]:
        if not group_text.startswith("{") or not group_text.endswith("}"):
            return group_text
        parts = group_text[1:-1].rpartition(", ")
        return {"name": parts[0].removeprefix("name: "), "uuid": parts[-1].removeprefix("uuid: ")}

    if not input:
        return input

    if input.startswith("[") and input.endswith("]"):
        # It looks like a list...
        # Remove [ and ] from the string
        t1 = input[1:-1]

        if not t1.startswith("{") or not t1.endswith("}"):
            # It looks like a a list of plain texts...
            result = []
            for item in t1.split(", "):
                result.append(item)
            return result

        # It looks like a a list of objects...
        # Split into { ... } group strings
        t2 = [item if item[-1] == "}" else item + "}" for item in t1.split("}, ")]
        # Convert group string into objects
        result = []
        for item in t2:
            result.append(group_to_object(item))
        return result

    elif input.startswith("{") and input.endswith("}"):
        # It looks like an object...
        return group_to_object(input)
        
    else:
        # It looks like plain text...
        return input


class Command(BaseCommand):
    help = (
        "Import Rapidpro data from a Remote API. "
        "But *first* you have to load in dashboard the exported data file (ie: u-report-romania.json), "
        "then load from console the locations by running: "
        "python3 manage.py import_geojson admin_level_0_simplified.json admin_level_1_simplified.json"
    )

    @staticmethod
    def clean_api_url(url: str) -> str:
        """Cleans up the API URL provided by the user"""
        if not url:
            return ""
        return url.removesuffix("/").removesuffix("/api/v2").strip()

    @staticmethod
    def clean_api_key(key: str) -> str:
        """Cleans up the API Key provided by the user"""
        if not key:
            return ""
        return key.lower().removeprefix("token").strip()

    @staticmethod
    def inverse_choices(mapping: Iterable[tuple[str, Iterable]]) -> list[dict[str, str]]:
        """Inverse lookup to find the CHOICES key from the provided value"""
        result: dict[str, str] = {}
        for row in mapping:
            result[row[0]] = {v: k for k, v in row[1]}
        return result

    @property
    def default_fields(self) -> dict[str, Any]:
        return {
            "is_system": False,
            "org": self.default_org,
            "created_by": self.default_user,
            "modified_by": self.default_user,
        }

    def throttle(self) -> None:
        """Pause the execution thread for a few seconds"""
        if self.throttle_requests:
            SECONDS = 5
            logger.info("Taking a %d second pause.", SECONDS)
            time.sleep(SECONDS)

    def __init__(self, *args, **kwargs):
        self.default_org = None
        self.default_user = None
        self.throttle_requests = False
        
        self.group_cache = {
            # "group_name": CacheItem(),
        }

        super().__init__(*args, **kwargs)

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "api_url",
            type=str,
            help="Remote API host (ie: https://rapidpro.ilhasoft.mobi)",
        )
        parser.add_argument(
            "api_key",
            type=str,
            help="Remote API key (ie: abcdef1234567890abcdef1234567890)",
        )
        parser.add_argument(
            "admin_user",
            type=str,
            help="Admin user name (for data not published by the API)",
        )
        parser.add_argument(
            "admin_pass",
            type=str,
            help="Admin user password (for data not published by the API)",
        )
        parser.add_argument(
            "--throttle",
            action="store_true",
            help="Slow down the API interrogations by taking some pauses",
        )

    def handle(self, *args, **options) -> None:
        api_url = Command.clean_api_url(options.get("api_url", os.environ.get("REMOTE_API_URL", "")))
        api_key = Command.clean_api_key(options.get("api_key", os.environ.get("REMOTE_API_KEY", "")))
        admin_user = options.get("admin_user", os.environ.get("REMOTE_ADMIN_USER", ""))
        admin_pass = options.get("admin_pass", os.environ.get("REMOTE_ADMIN_PASS", ""))
        
        self.client = TembaClient(api_url, api_key)
        self.web = WebSession.create_web_session(api_url, admin_user, admin_pass)

        # Use the first admin user we can find in the destination database
        try:
            self.default_user = User.objects.exclude(username="AnonymousUser").order_by("pk").all()[0]  # type: User
        except IndexError:
            self.write_error("You must first create an user from the frontend")
            return
        else:
            self.write_success("Default User = %s" % self.default_user)

        # Use the first organization we can find in the destination database
        self.default_org = Org.objects.filter(is_active=True, is_anon=False).all()[0]  # type: Org
        self.write_success("Default Org = %s" % self.default_org)

        if options.get("throttle"):
            self.throttle_requests = True

        # Copy the remaining data from the remote API
        # The order in which we copy the data is important because of object relationships

        copy_result = self._copy_groups()
        self.write_success("Copied %d new groups." % copy_result)

        # if Contact.objects.count():
        #     self.write_notice("Skipping contacts.")
        # else:
        # copy_result = self._copy_contacts()
        # self.write_success("Copied %d contacts." % copy_result)

        # if Archive.objects.count():  # TODO: copy the actual files?
        #     self.write_notice("Skipping archives.")
        # else:
        # copy_result = self._copy_archives()
        # self.write_success("Copied %d archives." % copy_result)

        # if Channel.objects.count():  # TODO: check channel association by name
        #     self.write_notice("Skipping channels.")
        # else:
        # copy_result = self._copy_channels()
        # self.write_success("Copied %d channels. You have to set the channel type from the shell!" % copy_result)

        # if Label.objects.count():
        #     self.write_notice("Skipping labels.")
        # else:
        # copy_result = self._copy_labels()
        # self.write_success("Copied %d labels." % copy_result)

        # if Broadcast.objects.count():  # TODO: Reset primary key sequence
        #     self.write_notice("Skipping broadcasts.")
        # else:
        # copy_result = self._copy_broadcasts()
        # self.write_success("Copied %d broadcasts." % copy_result)

        # if Msg.objects.count():  # TODO: Reset primary key sequence
        #     self.write_notice("Skipping messages.")
        # else:
        # copy_result = self._copy_messages()
        # self.write_success("Copied %d messages." % copy_result)

        # if ChannelEvent.objects.count():
        #     self.write_notice("Skipping channel events.")
        # else:
        # copy_result = self._copy_channel_events()
        # self.write_success("Copied %d channel events." % copy_result)

        # copy_result = self._copy_users()
        # self.write_success("Copied or updated %d users." % copy_result)

        # if FlowStart.objects.count():
        #     self.write_notice("Skipping flow starts.")
        # else:
        # copy_result = self._copy_flow_starts()
        # self.write_success("Copied %d flow starts." % copy_result)

        # if FlowRun.objects.count():
        #     self.write_notice("Skipping flow runs.")
        # else:
        copy_result = self._copy_flow_runs()
        self.write_success("Copied %d flow runs." % copy_result)

        copy_result = self._copy_flow_category_counts()
        self.write_success("Copied %d flow category counts." % copy_result)

        copy_result = self._fix_flow_run_counts()
        self.write_success("Fixed %d flow run counts." % copy_result)


    def write_success(self, message: str) -> None:
        self.stdout.write(self.style.SUCCESS(message))

    def write_error(self, message: str) -> None:
        self.stdout.write(self.style.ERROR(message))

    def write_notice(self, message: str) -> None:
        self.stdout.write(self.style.NOTICE(message))

    @property
    @cache
    def _get_groups_name_pk(self) -> Dict[UUID, ID]:
        """Retrieve all existing Group names and their corresponding database id"""
        return {item[0]: item[1] for item in ContactGroup.objects.values_list("name", "pk")}

    @property
    @cache
    def _get_contacts_uuid_pk(self) -> Dict[UUID, ID]:
        """Retrieve all existing Contact uuids and their corresponding database id"""
        return {item[0]: item[1] for item in Contact.objects.values_list("uuid", "pk")}

    @property
    @cache
    def _get_urns_pk(self) -> Dict[UUID, ID]:
        """Retrieve all existing URNs and their corresponding database id"""
        return {item[0]: item[1] for item in ContactURN.objects.values_list("identity", "pk")}

    @property
    @cache
    def _get_channels_name_pk(self) -> Dict[str, ID]:
        """Retrieve all existing Channel names and their corresponding database id"""
        return {item[0]: item[1] for item in Channel.objects.values_list("name", "pk")}

    @property
    @cache
    def _get_labels_uuid_pk(self) -> Dict[UUID, ID]:
        """Retrieve all existing Label uuids and their corresponding database id"""
        return {item[0]: item[1] for item in Label.objects.values_list("uuid", "pk")}

    @property
    @cache
    def _get_flows_name_pk(self) -> Dict[UUID, ID]:
        """Retrieve all existing Flow names and their corresponding database id"""
        return {item[0]: item[1] for item in Flow.objects.values_list("name", "pk")}

    @property
    @cache
    def _get_flowstarts_uuid_pk(self) -> Dict[UUID, ID]:
        """Retrieve all existing Flow Start uuids and their corresponding database id"""
        return {item[0]: item[1] for item in FlowStart.objects.values_list("uuid", "pk")}


    def _copy_archives(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices((("period", serializers.ArchiveReadSerializer.PERIODS.items()),))

        for read_batch in self.client.get_archives().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Archive] = []
            row: client_types.Archive
            for row in read_batch:
                # Older Temba versions use the "download_url" instead of "url"
                url = row.download_url if not hasattr(row, "url") else row.url
                
                # Remove the extra URL parameters
                url = url.split("?", 1)[0]

                item_data = {
                    "org": self.default_org,
                    "archive_type": row.archive_type,
                    "start_date": row.start_date,
                    "period": inverse_choice["period"][row.period],
                    "record_count": row.record_count,
                    "size": row.size,
                    "hash": row.hash,
                    "url": url,
                    "build_time": 0,
                }
                # TODO: Download and move the actual archive file
                item = Archive(**item_data)
                creation_queue.append(item)
            total += len(Archive.objects.bulk_create(creation_queue))
            logger.info("Total archives bulk created: %d.", total)
            self.throttle()
        return total

    def _copy_groups(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices((("status", serializers.ContactGroupReadSerializer.STATUSES.items()),))

        existing_names = list(ContactGroup.objects.all().values_list("name", flat=True))

        for read_batch in self.client.get_groups().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[ContactGroup] = []
            row: client_types.Group
            for row in read_batch:
                self.group_cache[row.name] = CacheItem(None, None, row.uuid)
                if row.name and row.name in existing_names:
                    continue

                item_data = {
                    **self.default_fields,
                    "name": row.name,
                    "query": row.query,
                    "status": inverse_choice["status"][row.status],
                    "is_system": False,
                    # TODO: The API doesn't give us the group type so we assume they're all 'Manual'
                    "group_type": ContactGroup.TYPE_MANUAL,
                }
                item = ContactGroup(**item_data)
                creation_queue.append(item)

            total += len(ContactGroup.objects.bulk_create(creation_queue))
            logger.info("Total groups bulk created: %d.", total)
            self.throttle()

        for group in ContactGroup.objects.all():
            if group.name in self.group_cache:
                old_uuid = self.group_cache[group.name].old_uuid
            else:
                old_uuid = None
            self.group_cache[group.name] = CacheItem(group.pk, group.uuid, old_uuid)
        return total

    def _copy_contacts(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices((("status", serializers.ContactReadSerializer.STATUSES.items()),))

        fields_key_field = { 
            field.key : field for field in ContactField.objects.all()}

        for read_batch in self.client.get_contacts().iterfetches(retry_on_rate_exceed=True):
            contact_uuid_group_names: dict[UUID, list[str]] = {}  # dict[ContactUUID, list[GroupName]]
            contact_urns: dict[UUID, list[str]] = {}
            creation_queue: list[Contact] = []
            row: client_types.Contact
            for row in read_batch:
                item_data = {
                    "org": self.default_org,
                    "created_by": self.default_user,
                    "modified_by": self.default_user,
                    "uuid": row.uuid,
                    "name": row.name,
                    "language": row.language,
                    "fields": {},
                    "created_on": row.created_on,
                    "modified_on": row.modified_on,
                    "last_seen_on": row.last_seen_on,
                }
                if not hasattr(row, "status") or row.status is None:
                    # The remote API is a Temba install older than v7.3.58 which doesn't have a status field
                    item_data |= {
                        "status": Contact.STATUS_BLOCKED
                        if row.blocked
                        else Contact.STATUS_STOPPED
                        if row.stopped
                        else Contact.STATUS_ACTIVE
                    }
                else:
                    # The remote API is newer Temba install
                    item_data |= {"status": inverse_choice["status"][row.status] if row.status else None}

                if row.fields:
                    for field_key in row.fields.keys():
                        field = fields_key_field.get(field_key)
                        if field:
                            item_data["fields"][str(field.uuid)] = {
                                ContactField.ENGINE_TYPES[field.value_type]: row.fields.get(field_key)
                            }

                item = Contact(**item_data)
                creation_queue.append(item)

                # current contact's URNs
                contact_urns[row.uuid] = row.urns

                # current contact's group memberships
                contact_uuid_group_names[row.uuid] = []
                for g in row.groups:
                    contact_uuid_group_names[row.uuid].append(g.name)

            contacts_created = Contact.objects.bulk_create(creation_queue)
            total += len(contacts_created)
            logger.info("Total contacts bulk created: %d.", total)

            group_through_queue: list[Model] = []  # the m2m "through" objects
            contact_urns_queue: list[ContactURN] = []  # the ContactURN objects
            for contact in contacts_created:
                for group_name in contact_uuid_group_names[contact.uuid]:
                    gid = self.group_cache[group_name].pk
                    # Use the Django's "through" table and bulk add the contact_id + contactgroup_id pairs
                    group_through_queue.append(Contact.groups.through(contact_id=contact.id, contactgroup_id=gid))
                for urn in contact_urns[contact.uuid]:
                    urn_scheme, urn_path, urn_query, urn_display = URN.to_parts(urn)
                    contact_urns_queue.append(
                        ContactURN(
                            org=self.default_org,
                            contact=contact,
                            scheme=urn_scheme,
                            path=urn_path,
                            identity=urn,
                            display=urn_display,
                        )
                    )
            Contact.groups.through.objects.bulk_create(group_through_queue)
            ContactURN.objects.bulk_create(contact_urns_queue)
            logger.info("Added groups and URNs to the created contacts.")
            self.throttle()
        return total

    def _copy_channels(self) -> int:
        total = 0
        for read_batch in self.client.get_channels().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Channel] = []
            row: client_types.Channel
            for row in read_batch:
                item_data = {
                    "org": self.default_org,
                    "created_by": self.default_user,
                    "modified_by": self.default_user,
                    "uuid": row.uuid,
                    "name": row.name,
                    "created_on": row.created_on,
                    "last_seen": row.last_seen,
                    "address": row.address,
                    "country": row.country,
                    "device": row.device,  # TODO
                    # "secret": "",  # TODO
                }
                # TODO: channel_type?
                # TODO: config?
                item = Channel(**item_data)
                creation_queue.append(item)
            total += len(Channel.objects.bulk_create(creation_queue))
            logger.info("Total channels bulk created: %d.", total)
            self.throttle()
        return total

    def _copy_channel_events(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices(
            (("event_type", serializers.ChannelEventReadSerializer.TYPES.items()),)
        )

        channels_name_pk = self._get_channels_name_pk
        contacts_uuid_pk = self._get_contacts_uuid_pk

        for read_batch in self.client.get_channel_events().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[ChannelEvent] = []
            row: client_types.ChannelEvent
            for row in read_batch:
                # Skip channel events for channels which don't seem to exist anymore
                if row.channel.name not in channels_name_pk:
                    logger.warning(
                        "Skipping channel events for channel %s %s",
                        row.channel.uuid,
                        row.channel.name,
                    )
                    continue
                item_data = {
                    "org": self.default_org,
                    "id": row.id,
                    "event_type": inverse_choice["event_type"][row.type],
                    "contact_id": contacts_uuid_pk.get(row.contact.uuid, None) if row.contact else None,
                    "channel_id": channels_name_pk[row.channel.name] if row.channel else None,
                    "extra": row.extra,
                    "occurred_on": row.occurred_on,
                    "created_on": row.created_on,
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
                    "org": self.default_org,
                    "created_by": self.default_user,
                    "modified_by": self.default_user,
                    "uuid": row.uuid,
                    "name": row.name,
                }
                item = Label(**item_data)
                creation_queue.append(item)
            total += len(Label.objects.bulk_create(creation_queue))
            logger.info("Total labels bulk created: %d.", total)
            self.throttle()
        return total

    def _copy_broadcasts(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices((("status", serializers.BroadcastReadSerializer.STATUSES.items()),))

        # This could use a lot of memory
        contacts_uuid_pk = self._get_contacts_uuid_pk
        urns_pk = self._get_urns_pk

        for read_batch in self.client.get_broadcasts().iterfetches(retry_on_rate_exceed=True):
            broadcast_id_group_names: dict[ID, list[str]] = {}  # dict[BroadcastID, list[GroupName]]
            contact_urns: dict[ID, list[str]] = {}
            contact_uuids: dict[ID, list[UUID]] = {}
            creation_queue: list[Broadcast] = []

            row: client_types.Broadcast
            for row in read_batch:
                item_data = {
                    "id": row.id,
                    "org": self.default_org,
                    "created_by": self.default_user,
                    "created_on": row.created_on,
                    "status": inverse_choice["status"][row.status],
                    "text": row.text,
                }
                item = Broadcast(**item_data)
                creation_queue.append(item)

                contact_urns[row.id] = row.urns
                broadcast_id_group_names[row.id] = []
                for g in row.groups:
                    broadcast_id_group_names[row.id].append(g.name)
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
                for gname in broadcast_id_group_names[broadcast.id]:
                    gid = self.group_cache[gname].pk
                    group_through_queue.append(Broadcast.groups.through(broadcast_id=broadcast.id, contactgroup_id=gid))
                for cuuid in contact_uuids[broadcast.id]:
                    cid = contacts_uuid_pk.get(cuuid, None)
                    contact_through_queue.append(Broadcast.contacts.through(broadcast_id=broadcast.id, contact_id=cid))
                for urn in contact_urns[broadcast.id]:
                    uid = urns_pk.get(urn, None)
                    urn_through_queue.append(Broadcast.urns.through(broadcast_id=broadcast.id, urn_id=uid))

            Broadcast.groups.through.objects.bulk_create(group_through_queue)
            Broadcast.contacts.through.objects.bulk_create(contact_through_queue)
            Broadcast.urns.through.objects.bulk_create(urn_through_queue)
            logger.info("Added groups, contacts, and URNs to created broadcasts.")
            self.throttle()
        return total

    def _copy_messages(self) -> int:
        total = 0
        contacts_uuid_pk = self._get_contacts_uuid_pk
        channels_name_pk = self._get_channels_name_pk
        labels_uuid_pk = self._get_labels_uuid_pk
        urns_pk = self._get_urns_pk

        inverse_choice = Command.inverse_choices(
            (
                ("direction", [(Msg.DIRECTION_IN, "in"), (Msg.DIRECTION_OUT, "out")]),
                ("type", serializers.MsgReadSerializer.TYPES.items()),
                ("status", serializers.MsgReadSerializer.STATUSES.items()),
                ("visibility", serializers.MsgReadSerializer.VISIBILITIES.items()),
            )
        )

        for read_batch in self.client.get_messages().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[Msg] = []
            label_uuids: dict[ID, list[UUID]] = {}

            row: client_types.Message
            for row in read_batch:
                item_data = {
                    "org": self.default_org,
                    "id": row.id,
                    "broadcast_id": row.broadcast,
                    "direction": inverse_choice["direction"][row.direction],
                    "msg_type": inverse_choice["type"][row.type],
                    "status": inverse_choice["status"][row.status],
                    "visibility": inverse_choice["visibility"][row.visibility],
                    "contact_id": contacts_uuid_pk.get(row.contact.uuid, None) if row.contact else None,
                    "contact_urn_id": urns_pk.get(row.urn, None) if row.urn else None,
                    "channel_id": channels_name_pk.get(row.channel.name, None) if row.channel else None,
                    "attachments": [],
                    "created_on": row.created_on,
                    "sent_on": row.sent_on,
                    "modified_on": row.modified_on,
                    "text": row.text,
                }

                for attachment in row.attachments:
                    content_type = attachment["content_type"]
                    source_url = attachment["url"]
                    destination_url = source_url  # TODO: download file from source_url and upload to destinaton_url
                    item_data["attachments"].append("{}:{}".format(content_type, destination_url))

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
                    label_through_queue.append(Msg.labels.through(msg_id=msg.id, label_id=lid))
            Msg.labels.through.objects.bulk_create(label_through_queue)
            logger.info("Added labels to created messages.")
            self.throttle()
        return total

    def _copy_users(self) -> int:
        total = 0
        inverse_choice = Command.inverse_choices((("role", serializers.UserReadSerializer.ROLES.items()),))

        for read_batch in self.client.get_users().iterfetches(retry_on_rate_exceed=True):
            row: client_types.User
            for row in read_batch:
                item_data = {
                    "email": row.email,
                    "first_name": row.first_name,
                    "last_name": row.last_name,
                    "date_joined": row.created_on,
                }
                org_role = inverse_choice["role"][row.role]
                item, created = User.objects.get_or_create(username=row.email, defaults=item_data)
                # Save users one by one instead of doing it in batches
                item.save()
                self.default_org.add_user(item, org_role)
                total += 1
            logger.info("Total users created or updated: %d.", total)
            self.throttle()
        return total

    def _copy_flow_starts(self) -> int:
        inverse_choice = Command.inverse_choices((("status", serializers.FlowStartReadSerializer.STATUSES.items()),))
        flows_name_pk = self._get_flows_name_pk
        groups_name_pk = self._get_groups_name_pk
        contacts_uuid_pk = self._get_contacts_uuid_pk

        total = 0
        for read_batch in self.client.get_flow_starts().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[FlowStart] = []
            group_names: dict[UUID, list[str]] = {}
            contact_uuids: dict[UUID, list[UUID]] = {}
            row: client_types.FlowStart
            for row in read_batch:
                if row.flow.name not in flows_name_pk:
                    logger.warning(
                        'Skipping Flow Start "%s" because Flow "%s" does not exist',
                        row.uuid,
                        row.flow.name,
                    )
                    continue

                item_data = {
                    "org": self.default_org,
                    "created_by": self.default_user,
                    "uuid": row.uuid,
                    "created_on": row.created_on,
                    "modified_on": row.modified_on,
                    "flow_id": flows_name_pk.get(row.flow.name, None),
                    "status": inverse_choice["status"][row.status],
                    "restart_participants": row.restart_participants,
                    "include_active": not row.exclude_active,
                    "extra": row.extra,
                    #  'params': row.params,  # this seems to be an alias for row.extra
                }

                item = FlowStart(**item_data)
                creation_queue.append(item)

                group_names[row.uuid] = []
                for group in row.groups:
                    group_names[row.uuid].append(group.name)

                contact_uuids[row.uuid] = []
                for contact in row.contacts:
                    contact_uuids[row.uuid].append(contact.uuid)

            flow_starts_created = FlowStart.objects.bulk_create(creation_queue)
            total += len(flow_starts_created)
            logger.info("Total flow starts bulk created: %d.", total)

            group_through_queue: list[Model] = []
            contact_through_queue: list[Model] = []
            for flow_start in flow_starts_created:
                for gname in group_names[flow_start.uuid]:
                    gid = groups_name_pk.get(gname, None)
                    group_through_queue.append(
                        FlowStart.groups.through(flowstart_id=flow_start.id, contactgroup_id=gid)
                    )
                for cuuid in contact_uuids[flow_start.uuid]:
                    cid = contacts_uuid_pk.get(cuuid, None)
                    if cid:
                        contact_through_queue.append(
                            FlowStart.contacts.through(flowstart_id=flow_start.id, contact_id=cid)
                        )
                    else:
                        logger.warning('FlowStart cannot find contact with UUID "%s"', cuuid)
            FlowStart.contacts.through.objects.bulk_create(contact_through_queue)
            logger.info("Added contacts to created flow starts.")
            FlowStart.groups.through.objects.bulk_create(group_through_queue)
            logger.info("Added groups to created flow starts.")

            self.throttle()
        return total

    def _copy_flow_runs(self) -> int:
        inverse_choice = Command.inverse_choices((("exit_type", serializers.FlowRunReadSerializer.EXIT_TYPES.items()),))
        flows_name_pk = self._get_flows_name_pk
        flowstarts_uuid_pk = self._get_flowstarts_uuid_pk
        contacts_uuid_pk = self._get_contacts_uuid_pk
        total = 0
        
        def translate_group_uuids(data):
            """
            Data must be either a list of dicts or a dict:
                [{"name": "asdasd", "uuid": "12345"}]
            """
            if not data:
                return data
            
            if type(data) == list:
                for i, item in enumerate(data):
                    if "uuid" in data[i]:
                        data[i]["uuid"] = self.group_cache[item["name"]].uuid
            elif type(data) == list:
                if "uuid" in data[i]:
                    data[i]["uuid"] = self.group_cache[item["name"]].uuid
            return 
            
        flow_results_key_uuid = {}  # map ResultKey to UUID
        flow_results_old_uuid = {}  # map OLD-UUID to UUID
        for flow in Flow.objects.all():
            for r in flow.metadata["results"]:
                flow_results_key_uuid[r["key"]] = r["node_uuids"][0]

        for read_batch in self.client.get_runs().iterfetches(retry_on_rate_exceed=True):
            creation_queue: list[FlowRun] = []
            row: client_types.Run
            for row in read_batch:
                # Skip flow runs which do not belong to any flow
                if not row.flow or not flows_name_pk.get(row.flow.name, None):
                    logger.warning(
                        'Skipping Flow Run "%s" because its Flow "%s" does not exist', 
                        row.uuid, 
                        row.flow.name
                    )
                    continue

                flow = Flow.objects.get(pk=flows_name_pk.get(row.flow.name, None))
                flow_deps_category = {}
                for d in flow.metadata["dependencies"]:
                    flow_deps_category[d["name"]] = d

                item_results = {}
                for k, r in row.values.items():
                    parsed_input = parse_broken_json(r.input)
                    parsed_value = parse_broken_json(r.value)

                    # Fix the group UUIDs if needed
                    dependency = flow_deps_category.get(r.category, None)
                    if dependency and dependency.get("type", "") == "group":
                        parsed_input = translate_group_uuids(parsed_input)
                        parsed_value = translate_group_uuids(parsed_value)

                    node_uuid = flow_results_key_uuid.get(k, None)
                    if not node_uuid:
                        node_uuid = r.node
                        logger.warning('Cannot translate result node uuid for key %s', k)

                    flow_results_old_uuid[r.node] = node_uuid

                    item_results[k] = {
                        "node_uuid": node_uuid,
                        "name": r.name,
                        "created_on": r.time,
                        "input": parsed_input,
                        "value": parsed_value,
                        "category": r.category,
                    }

                # TODO: This only maps nodes which are set in the current Flow's Results. It skips unknown results.
                item_path = []
                for i, step in enumerate(row.path):
                    step_node_uuid = flow_results_old_uuid.get(step.node)
                    if step_node_uuid:
                        item_path.append({
                            "node_uuid": step_node_uuid,
                            "arrived_on": step.time,
                            "exit_uuid": None
                        })
                
                # Set the exit_uuid to the next node uuid  
                # TODO: not sure if this is ok
                if len(item_path) > 1:
                    for i, step in enumerate(item_path):
                        if i == 0:
                            continue
                        item_path[i-1]["exit_uuid"] = item_path[i]["node_uuid"]

                item_data = {
                    "org": self.default_org,
                    "uuid": row.uuid,
                    "created_on": row.created_on,
                    "modified_on": row.modified_on,
                    "flow_id": None if not row.flow else flows_name_pk.get(row.flow.name, None),
                    "contact_id": None if not row.contact else contacts_uuid_pk.get(row.contact.uuid, None),
                    "start_id": None if not row.start else flowstarts_uuid_pk.get(row.start.uuid, None),
                    "responded": row.responded,
                    "path": item_path,
                    "results": item_results,
                    "exited_on": row.exited_on,
                    "status": "" if not row.exit_type else inverse_choice["exit_type"][row.exit_type],
                }
                item = FlowRun(**item_data)
                creation_queue.append(item)

            flow_runs_created = FlowRun.objects.bulk_create(creation_queue)
            total += len(flow_runs_created)
            logger.info("Total flow runs bulk created: %d.", total)
            self.throttle()
        return total

    def _copy_flow_category_counts(self) -> int:
        total = 0
        
        FlowCategoryCount.objects.all().delete()
        logger.info("Deleted flow category counts")

        flow_results_key_uuid = {}
        for flow in Flow.objects.all():
            for r in flow.metadata["results"]:
                flow_results_key_uuid[r["key"]] = r["node_uuids"][0]

        for read_batch in self.client.get_flows().iterfetches(retry_on_rate_exceed=True):
            remote_data: client_types.Flow
            for remote_data in read_batch:
                # "uuid": remote_data.uuid
                # "name": remote_data.name
                creation_queue: list[FlowCategoryCount] = []
                web_response = self.web.get("/flow/category_counts/{}/".format(remote_data.uuid))
                
                try:
                    flow = Flow.objects.get(name=remote_data.name)
                except Flow.DoesNotExist:
                    logger.warning("Cannot find Flow: %s", remote_data.name)
                    continue

                if web_response.status_code != 200:
                    logger.warning(
                        "HTTP Status %s when retrieving category counts for Flow %s",
                            web_response.status_code, 
                            flow.uuid
                        )
                    continue

                counts = web_response.json().get("counts", {})
                for count in counts:
                    for cat in count["categories"]:
                        item = FlowCategoryCount(
                            flow=flow,
                            result_key=count["key"],
                            result_name=count["name"],
                            category_name=cat["name"],
                            count=cat["count"],
                            node_uuid=flow_results_key_uuid[count["key"]],
                        )
                        creation_queue.append(item)

                flow_counts_created = FlowCategoryCount.objects.bulk_create(creation_queue)
                total += len(flow_counts_created)
                logger.info("Total flow category counts bulk created: %d.", total)
                self.throttle()
                
        return total

    def _fix_flow_run_counts(self) -> int:
        total = 0
        
        FlowRunCount.objects.all().delete()
        logger.info("Deleted flow run counts")

        for read_batch in self.client.get_flows().iterfetches(retry_on_rate_exceed=True):
            remote_data: client_types.Flow
            creation_queue: list[FlowRunCount] = []
            
            for remote_data in read_batch:
                try:
                    flow = Flow.objects.get(name=remote_data.name)
                except Flow.DoesNotExist:
                    logger.warning("Cannot find Flow: %s", remote_data.name)
                    continue
                creation_queue.append(FlowRunCount(flow=flow, count=remote_data.runs.completed, exit_type="C"))
                creation_queue.append(FlowRunCount(flow=flow, count=remote_data.runs.interrupted, exit_type="I"))
                creation_queue.append(FlowRunCount(flow=flow, count=remote_data.runs.expired, exit_type="E"))
                # creation_queue.append(FlowRunCount(flow=flow, count=remote_data.runs.failed???, exit_type="F"))

            flow_counts_created = FlowRunCount.objects.bulk_create(creation_queue)
            total += len(flow_counts_created)
            logger.info("Total flow run counts bulk created: %d.", total)
        
        self.throttle()
        return total
