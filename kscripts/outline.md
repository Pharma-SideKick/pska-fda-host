# General outline of the pipeline for ingesting data from the DailyMed exports.

## Cleaning up Archives

0. (Optional) Download the exports from the DailyMed site.
   1. The data is available in zip files, and there are 5 main exports. They are updated occasionally, so there will eventually be a need to update these.
   2. The zip files contain individual archives for each medicine, with each subarchive containing a single XML file for that medicine.
   3. Some files are compressed with gzip, and some are not. The script will need to handle both cases.
1. Main Zip: unzip the zip file and extract the subarchives to a directory.
2. Subarchives: unzip the subarchives and extract the XML files to a directory.
   1. As stated above, some files are compressed with gzip, and some are not. The script will need to handle both cases.
3. XML files: parse the XML files and extract the data into a directory.
   1. As state above, decompress with gzip if necessary.
   2. Utilize NodeJS with Cheerio (HTML/XML parsing library) to parse the XML files.
      1. XML data will need to be explored to see the correct parsing strategy. Data will need to be traversed and extracted.
      2. TRICKY PART: How we format the JSON data will matter for the embeddings at the end. This will need some iterations to see what the best approach is.
      3. The script at this step can either make calls directly out to the database (Couchbase?) or it can return the data to Python for further processing.
   3. Once the data is parsed, it can be written to a file or a database. Maybe make this a choice via a command line argument.

### Exploratory: Control Panel for Pipelines

There's a few things I would like to do to make the data pipeline more robust. For example, we could initiate a Flask server that spawns Luigi processes. This could be a handy way
to show the user the controls for the pipelines, and possibly status. The problem we may face here is that Flask needs something like SocketIO to properly return WebSocket requests.
Combining this with the need to have a nice user panel with something like React, we may as well use a NodeJS webserver as the "controller" or "control panel". We can communicate between
the NodeJS and Python processes sockets: UNIX sockets if they are available, or TCP/IP sockets if they are connecting between docker containers. Essentially we will make a controller for
Node to Python RPCs (Remote Procedure Calls). So we'll have a NodeJS app that acts as the host, and Python/Flask will act as the controller.

The steps can be manually fired off, or eventually ordered to fire off altogether.
