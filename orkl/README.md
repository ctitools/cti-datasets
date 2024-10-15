# Fetching the ORKL.eu dataset

## Pre-requisite:

```bash
pip install -U -r requirements.txt
```

Next, fetch the data: this might take a night or so.

```bash
#!/bin/bash


BASEURL="https://archive.orkl.eu/"

wget -O all.tar.gz 'https://archive.orkl.eu/?download=tar_gz'


for f in "library.json" "tas.json"; do
	wget -O $f "${BASEURL}$f"
done
```

Then you will have 3 files: 
- `all.tar.gz`  ... all the .txt and PDF documents
- `library.json` ... the JSON representation of the library metadata
- `tas.json` .... Threat Actors JSON representation



# How to import the data into a PostgreSQL database?

1. Create the database: `createdb orkl`
2. Make sure you fetched library.json. Next, insert the data: `python insert-db.py`
3. Verify that the library.json was validated against the library.schema.json (JSON schema) and that all rows were inserted properly.

# Profit

Take a look at the library.schema.json and/or the postgreSQL database (or db.sql for the SQL schema).
You can now SELECT texts based on language, translate them, GROUP by tools, threat actors (TAs), cluster by TA aliases, etc. etc...


