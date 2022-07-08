.. ENTSOE/G-Monitor documentation master file, created by
   sphinx-quickstart on Fri Jul  8 00:34:16 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to ENTSOE/G-Monitor's documentation!
============================================

This is the documentation of the ENTSO-E and ENTSO-G monitor project.

It is split into a part for crawling the data and storing it in a TimescaleDB.
This is especially useful if the data is also used in other projects.

The second part is the visualization of the data with a Plotly Dash-board.
Check out the :doc:`usage` section for further information, including how to install the project.
.. toctree::
   :maxdepth: 4
   :caption: Contents:

   entsoe_crawler
   entsoe_dash
   entsoe_data_manager
   entsoe_sqlite_manager
   entsog_crawler
   entsog_dash
   entsog_data_manager
   entsog_maploader
   entsog_sqlite_manager


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
