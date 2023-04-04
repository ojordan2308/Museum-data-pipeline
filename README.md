# Pipeline

This folder contains python script for reading a kafka data stream, verifying and cleaning the data, and inserting it into a database.

## Installation

```console
pip3 install -r requirements.txt
```

## Environment

Requires root .env file with the following keys:

```
ACCESS_KEY_ID=a
SECRET_ACCESS_KEY=b
DATABASE_NAME=c
DATABASE_USERNAME=d
DATABASE_PASSWORD=e
DATABASE_IP=f
DATABASE_PORT=g
BOOTSTRAP_SERVERS=h
SASL_USERNAME=i
SASL_PASSWORD=j
```

## Development

2 optional flags:
--l :logs all erroneous data values to an error log txt file.
--e :begins kafka data stream from earliest entry rather than the latest.
