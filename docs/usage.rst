Usage
=====

To use this project, it is recommended to install Docker first.

Then, one can can run `docker compose build` to build the container.

Finally use `docker compose up -d` to start the dashboard and TimescaleDB container.

Then you can look if `first=True` in `updateDatabase` in `crawl_cron.py` is set (for the first time) and then run:

`docker exec entso-dashboard python crawl_cron.py > /usr/bin/logger -t entso-monitor`

to pull the data once.

This may take 2-4 days, as the ENTSO-E and ENTSO-G servers are often responding with 5XX errors.

After this, you have a complete mirror of all the data from ENTSO-E/G stored efficiently in a TimescaleDB.