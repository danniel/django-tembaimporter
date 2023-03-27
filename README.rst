=============
Temba Importer
=============

Temba Importer is a Django app for copying data from a running RapidPro Temba
installation to another one. This app must be added as a plugin to the 
destination installation and it requires API access to the source install.

The destination installation must have a single Org, a single AnonymousUser 
and a single Admin account created. Everything else must be deleted before import
(with the --flush parameter).

The destination installation Admin account must use an email address which does
not exist anywhere else in the RapidPro Temba database.


Quick start
-----------

1. Add "tembaimporter" to the destination install's settings file:

    INSTALLED_APPS = [
        ...
        'tembaimporter',
    ]

2. On the destination installation create a default RapidPro admin account and a default RapidPro organization.

    For example, on a blank install:
    
    2.1. Sign up by using the frontend form

    2.2. From console run
        
        ``python3 manage.py shell``

    2.3. You should see two users: the one you just created, and the anonymous account

        ``from django.contrib.auth.models import User``

        ``User.objects.all()``

    2.4 Set the user you just created to be an admin

        ``u = User.objects.all()[1]  # or whatever index it is``

        ``u.is_superuser = True``
        
        ``u.save()``

3. From console, cd to where the manage.py file is located and run:

    ``python3 manage.py tembaimport http://source.example.com SOURCE_API_KEY --flush --throttle``

4. The app does not copy the channel types because they are not exported by the API. They must be set manually.

    ``from temba.channels.models import Channel``

    ``chan1 = Channel.objects.all()[0]``

    ``chan1.channel_type = 'FB'  # For a FaceBook channel``
    
    ``chan1.config = {"auth_token":"", "page_name":"", "secret":"", "callback_domain":"example.com"}``
    
    ``chan1.save()``

    
    ``chan2 = Channel.objects.all()[1]``

    ``chan2.channel_type = 'TG'  # For a Telegram channel``
    
    ``chan2.config = {"auth_token":"", "callback_domain":"example.com"}``

    ``chan2.save()``

5. The app creates the System groups but it does not set the types for the other copied groups, because they are not exported by the API. By default it sets them as "M" ("Manual").

    ``from temba.contacts.models import ContactGroup``

    ``group = ContactGroup.objects.all()[0]``

    ``group.group_type = 'Q'  # For a "Smart" group``
    
    ``group.save()``
