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


3. From console, cd to where the manage.py file is located and run:

    python3 manage.py tembaimporter http://source.example.com SOURCE_API_KEY --flush --throttle

