var _ = require('lodash');
var merge = require('viewdb').merge;
var LegacyObserver = require('viewdb').Observer;
var Kuery = require('kuery');
var projectDocument = require('./utils').projectDocument;
var LoggerFactory = require('slf').LoggerFactory;

const log = LoggerFactory.getLogger('viewdb_persistence_store_mongodb:observer')

var Observer = function (query, queryOptions, collection, options, oplogListener) {
  var self = this;
  self._queryOptions = queryOptions;

  if (!oplogListener) {
    return new LegacyObserver(query, queryOptions, collection, options);
  }
  var namespace = collection._collection.s.namespace;
  this._query = query;
  this._options = options;
  // this._options = logger;
  this._collection = collection;
  this._cache = [];
  this.listener = undefined;
  this._kuery;

  self.loadInitial(function () {
    self.listener = oplogListener.listen(namespace, self._onOperation, self);
  });

  this._kuery = new Kuery(this._query.query);

  var dispose = function () {
    if (self.listener) {
      self.listener.dispose();
    }
    self._cache = null;
    return Promise.resolve();
  };
  return {
    stop: dispose,
    dispose: dispose
  };
};

var comparator = function (a, b) {
  return a._id === b._id;
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
  if (!this._cache) {
    log.warn('Got oplog event for document but cache was already disposed');
    return;
  }
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
      log.warn('Unhandled operation: %s', doc.op);
      break;
  }
};

Observer.prototype._checkKuery = function (coll) {
  var res = this._kuery.find(coll);
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
      var document = doc.o;
      var project = this._queryOptions.project;

      if (project) {
        document = projectDocument(document, project);
      }

      if (this._options.added) {
        this._options.added(document, length - 1);
      }
    }
  }
};

Observer.prototype._onUpdate = function (doc) {
  var match = this._checkKuery([doc.o]);
  if (match) {
    var index = this._cache.indexOf(doc.o._id);
    if (index !== -1) {
      this._cache[index] = doc.o._id;
    } else {
      var length = this._cache.push(doc.o._id);
      index = length - 1;
    }
    if (this._options.changed) {
      this._options.changed(null, doc.o, index); // have no access to asis / old document
    }
  } else {
    this._onRemove(doc);
  }
};

Observer.prototype._onRemove = function (doc) {
  var index = this._cache.indexOf(doc.o._id);
  if (index > -1) {
    this._cache.splice(index, 1);
    if (this._options.removed) {
      this._options.removed(doc.o, index);
    }
  }
};

module.exports = Observer;
