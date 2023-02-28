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

2. On the destination installation create a default RapidPro admin account and a default RapidPro organization.

    For example, on a blank install:
    
    2.1. Sign up by using the frontend form

    2.2. From console run
        
        ``python3 manage.py shell``

    2.3. You should see two users: the one you just created, and the anonymous account

        ``from django.contrib.auth.model import User``

        ``User.objects.all()``

    2.4 Set the user you just created to be an admin

        ``u = User.objects.all()[1]  # or whatever index it is``

        ``u.is_superuser = True``
        
        ``u.save()``

3. From console, cd to where the manage.py file is located and run:

    ``python3 manage.py tembaimporter http://source.example.com SOURCE_API_KEY --flush --throttle``

