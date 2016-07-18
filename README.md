# nosqlimport-mongodb

A module for [nosqlimport](https://www.npmjs.com/package/nosqlimport) that allows data to be published to CouchDB or Cloudant.

## Installation

```sh
npm install -g nosqlimport nosqlimport-mongodb
```

## Import data to CouchDB

```sh
cat test.tsv | nosqlimport -n mongodb -u mongodb://localhost:27017/mydatabase --db mycollection
```

See [nosqlimport](https://www.npmjs.com/package/nosqlimport) for further options.
