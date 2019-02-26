var _ = require('lodash');
var merge = require('viewdb').merge;
var MongoOplog = require('mongo-oplog');
var LegacyObserver = require('viewdb').Observer;
var Kuery = require('kuery');

var logger = {
  added: function() { console.log("added: "); console.log(arguments); },
  removed: function() { console.log("removed: "); console.log(arguments); },
  changed: function() { console.log("changed: "); console.log(arguments); },
  moved: function() { console.log("moved: "); console.log(arguments); },
};


var getMongoUrl = function (collection) {
  if (!collection._collection.s.db.s.topology.s.replset) {
    // no replica set
    var dbOptions = collection._collection.s.db.s.topology.s.clonedOptions;
    var oplogUri = 'mongodb://' + dbOptions.host + ':' + dbOptions.port + '/local';
    return oplogUri;
  } else {
    // in case of replica set
    var hosts = collection._collection.s.db.s.topology.s.replset.s.seedlist;
    var hostString = '';
    _.forEach(hosts, function (host, index) {
      if (index === 0) {
        hostString += host.host + ':' + host.port
      } else {
        hostString += ',' + host.host + ':' + host.port
      }
    });
    var replicaSet = collection._collection.s.db.s.topology.s.replset.s.options.setName;
    var oplogUri = 'mongodb://' + hostString + '/local?replicaSet=' + replicaSet;
    return oplogUri;
  }
};


var Observer = function (query, queryOptions, collection, options) {
  var self = this;
  self._queryOptions = queryOptions;
  if (!options.oplog) {
    return new LegacyObserver(query, queryOptions, collection, options);
  }

  var namespace = collection._collection.s.namespace;

  var oplogUri = getMongoUrl(collection);
  console.log('mongo-oplog connection url: ' + oplogUri);

  this._query = query;
  this._options = options;
  // this._options = logger;
  this._collection = collection;
  this._cache = [];

  this._oplog = MongoOplog(oplogUri, { ns: namespace });

  self._oplog.tail().then(function () {
    self.loadInitial(function () {
      self._oplog.on('op', self._onOperation.bind(self));
    });
  });

  return {
    stop: function () {
      self._oplog.destroy().then(function () {
        self._cache = null;
      });
    }
  }
};

var comparator = function (a, b) {
  return a._id === b._id
};

Observer.prototype.loadInitial = function (cb) {
  var self = this;
  var newQuery = _.merge(this._query, self._queryOptions);
  this._collection._getDocuments(newQuery, function (err, result) {
    if (self._options.init) {
      self._options.init(result);
    } else {
      merge(null, result, _.defaults({ comparatorId: comparator }, self._options));
    }
    self._cache = _.map(result, '_id');
    cb();
  });
};

Observer.prototype._onOperation = function (doc) {
  switch (doc.op) {
    case 'i':
      this._onInsert(doc);
      break;
    case 'u':
      this._onUpdate(doc);
      break;
    case 'd':
      this._onRemove(doc);
      break;
    default:
      console.log('WARN: Unhandled operation: ', doc.op);
      break;
  }
};

Observer.prototype._checkKuery = function (coll) {
  var q = new Kuery(this._query.query);
  var res = q.find(coll);
  return res && res.length > 0;
};

Observer.prototype._onInsert = function (doc) {
  var index = this._cache.indexOf(doc.o._id);
  var match = this._checkKuery([doc.o]);
  if (match) {
    if (index > -1) {
      // already in cache - user has been notified by loadInitial method
    } else {
      var length = this._cache.push(doc.o._id);
      if(this._options.added) {
        this._options.added(doc.o, length - 1);
      }
    }
  }
};

var isFindAndModifyOp = function (doc) {
  return _.has(doc, 'o.$set');
};

Observer.prototype._onUpdate = function (doc) {
  if (isFindAndModifyOp(doc)) {
    this._onUpdateFindAndModify(doc);
  } else {
    var match = this._checkKuery([doc.o]);
    var index = this._cache.indexOf(doc.o._id);
    if (match) {
      if (index !== -1) {
        this._cache[index] = doc.o._id;
      } else {
        console.log('WARN: Got update event for document that was not in cache. This can lead to possible issues when using index');
        var length = this._cache.push(doc.o._id);
        index = length - 1;
      }
      if (this._options.changed) {
        this._options.changed(null, doc.o, index); // have no access to asis / old document
      }
    } else {
      this._onRemove(doc);
    }
  }
};

Observer.prototype._onUpdateFindAndModify = function (doc) {
  var self = this;
  var id = doc.o2._id;
  self._collection.find({ _id: id }).toArray(function (err, results) {
    if (err) {
      console.log('ERROR.. TODO - IMPLEMENT RETRY ?', err);
    } else {
      var res = results[0];
      var match = self._checkKuery([res]);
      if (_.isNil(self._cache)) {
        console.log('WARN: _cache was undefined or null, this observer is most likely stopped. Aborting change notification.');
        return;
      }
      var index = self._cache.indexOf(res._id);
      if (match) {
        if (index !== -1) {
          self._cache[index] = res._id;
        } else {
          console.log('WARN: Got update event for document that was not in cache. This can lead to possible issues when using index');
          var length = self._cache.push(res._id);
          index = length - 1;
        }
        if (self._options.changed) {
          self._options.changed(null, res, index); // have no access to asis / old document
        }
      } else {
        self._onRemoveFindAndModify(res);
      }
    }
  })
};

Observer.prototype._onRemove = function (doc) {
  var index = this._cache.indexOf(doc.o._id);
  if (index > -1) {
    this._cache.splice(index, 1);
    if(this._options.removed) {
      this._options.removed(doc.o, index);
    }
  }
};

Observer.prototype._onRemoveFindAndModify = function (doc) {
  var index = this._cache.indexOf(doc._id);
  if (index > -1) {
    this._cache.splice(index, 1);
    if(this._options.removed) {
      this._options.removed(doc, index);
    }
  }
};

module.exports = Observer;
