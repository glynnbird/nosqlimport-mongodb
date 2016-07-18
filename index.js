var async = require('async'),
  MongoClient = require('mongodb').MongoClient,
  debug = require('debug')('nosqlimport');

debug('Using mongodb nosqlimport writer');

var writer = function(opts) {

  var stream = require('stream'),
    buffer = [ ],
    collection = null,
    written = totalfailed = 0,
    buffer_size = 500;
    parallelism = 5;
    

  var options = {};
  if (opts.url.indexOf("localhost")===-1) {
    opts.url = opts.url + "?ssl=true";
    options = {
      server: {
        sslValidate: false
      },
      mongos: {
          ssl: true,
          sslValidate: false,
          poolSize: 1,
          reconnectTries: 1
        }
    };
  }  

  debug('MongoDB URL: ' + opts.url.replace(/\/\/.+@/g, "//****:****@"));
  debug('MongoDB Collection: ' + opts.database);

  MongoClient.connect(opts.url, options, function(err, db) {
    if (err) {
      throw("Could not connect to MongoDB database",err);
    }  
    console.log("connected to MongoDB")
    collection = db.collection(opts.database)
  });



  // process the writes in bulk as a queue
  var q = async.queue(function(payload, cb) {
    async.whilst(function() {
      return (collection === null)
    }, function(done) {
      setTimeout(done, 100);
    }, function() {
      collection.insert(payload, function(err, data) {
        if (err) {
          writer.emit('writeerror', err);
        } else {
          written ++;
          writer.emit("written", { documents: 1, failed: 0, total: written, totalfailed: totalfailed});
          debug({ documents: 1, failed: 0, total: written, totalfailed: totalfailed});
        }
        cb();
      });
    });

  }, parallelism);
  
  
  // write the contents of the buffer to CouchDB in blocks of 500
  var processBuffer = function(flush, callback) {
  
    if (flush || buffer.length>= buffer_size) {
      var toSend = buffer.splice(0, buffer.length);
      buffer = [];
      q.push(toSend);
      
      // wait until the buffer size falls to a reasonable level
      async.until(
        
        // wait until the queue length drops to twice the paralellism 
        // or until empty
        function() {
          if(flush) {
            return q.idle() && q.length() ==0
          } else {
            return q.length() <= parallelism * 2
          }
        },
        
        function(cb) {
          setTimeout(cb,100);
        },
        
        function() {
          if (flush) {
            writer.emit("writecomplete", { total: written , totalfailed: totalfailed});
          }
          callback();
        });


    } else {
      callback();
    }
  }

  var writer = new stream.Transform( { objectMode: true } );

  // take an object
  writer._transform = function (obj, encoding, done) {

    // add to the buffer, if it's not an empty object
    if (obj && typeof obj === 'object' && Object.keys(obj).length>0) {
      buffer.push(obj);
    }

    // optionally write to the buffer
    this.pause();
    processBuffer(false,  function() {
      done();
    });

  };

  // called when we need to flush everything
  writer._flush = function(done) {
    processBuffer(true, function() {
      done();
    });
  };
  
  return writer;
};

module.exports = {
  writer: writer
}