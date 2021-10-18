# ENTSO-E and ENTSO-G Monitor

This is a software project developed for the AMI Master at FH Jülich

## running the software

the software should be deployed with docker or directly as a python file

The databases must be created by running the crawl_cron.py file.

The visualization also needs to load map-data from ENTSOG.
This needs the execution of the script entsog_maploader.py

## updating

You can create a cronjob on the host to run

`docker exec entso-monitor crawl_cron.py > /usr/bin/logger -t entso-monitor`