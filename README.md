# metafacture-mongodb

Metafacture meets MongoDB

## About metafacture-mongodb

metafacture-mongodb is a plugin for [Metafacture](https://github.com/culturegraph/metafacture-core). It provides the  modules to-mongodb and from-mongodb to store and retrieve Metafacture streams to and from Mongo database collections.

## Key Features

* Stores Metafacture streams to a Mongo database collection
* Maps Metafacture records to MongoDB records
* Retrieves stored records by using record identifiers
* Keeps the hierachical structure of Metafacture records intact

## Download and Install

metafacture-mongodb can be used as a plugin in the Metafacture distribution or as a Java library in your own programs.

## Usage

Both modules need a [MongoURI](http://docs.mongodb.org/manual/reference/connection-string/) to connect to the Mongo database instance.

### to-mongodb

Acts as a `StreamReciever` and writes each stream record as MongoDB record to the given MongoDB collection. This module provides no output.

### from-mongodb

Acts as an `ObjectPipe<String, StreamReceiver>`. Each input string represents the identifier of one record, which is fetched from the given MongoDB collection and transformed to a Metafacture stream.

## Examples

The following Flux script opens a file, interprets the content as pica records and stores them to a MongoDB database.

```c
default fileName = FLUX_DIR + "10.pica";
default mongoUri = "mongodb://localhost/pica.records";

fileName|
open-file|
as-lines|
decode-pica|
to-mongodb(mongoUri);
```

This Flux script retrieves a record by its identifier and writes the stored stream to stdout:

```c
default identifier = "1234567890";
default mongoUri = "mongodb://localhost/pica.records";

identifier|
from-mongodb(mongoUri)|
encode-formeta(style="multiline")|
write("stdout");
```

