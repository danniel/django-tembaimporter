=============
Temba Importer
=============

Temba Importer is a Django app for copying data from a running RapidPro Temba
installation to another one. This app must be added as a plugin to the 
destination installation and it requires API access to the source install.



Quick start
-----------

1. Add "tembaimporter" to the destination install's settings file:

    INSTALLED_APPS = [
        ...
        'tembaimporter',
    ]

2. On the destination installation web dashboard you must sign up, then import your Flows' JSON.

3. From the destination installation console you must import the geolocation data files:

    ``python3 manage.py import_geojson admin_level_0_simplified.json admin_level_1_simplified.json``

3. From console, cd to where the manage.py file is located and run:

    ``python3 manage.py temba_api_import source.example.com SOURCE_API_KEY admin-username@example.com AdminAccountPassword``

4. The app does not copy the channel types because they are not exported by the API. They must be set manually.

    ``from temba.channels.models import Channel``

    ``chan1 = Channel.objects.all()[0]``

    ``chan1.channel_type = 'FBA'  # For a FaceBook App channel``
    
    ``chan1.config = {"auth_token":"", "page_name":"", "secret":"", "callback_domain":"example.com"}``
    
    ``chan1.save()``

    
    ``chan2 = Channel.objects.all()[1]``

    ``chan2.channel_type = 'TG'  # For a Telegram channel``
    
    ``chan2.config = {"auth_token":"", "callback_domain":"example.com"}``

    ``chan2.save()``



Fix attachment paths
-----------

After you move the media files to the new location,
you can update their URLs from the database by running:

``python3 manage.py temba_fix_attachment_path original.s3.us-east-1.amazonaws.com  new.s3.eu-west-1.amazonaws.com``
