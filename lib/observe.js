var _ = require('lodash');
var merge = require('viewdb').merge;
var MongoOplog = require('mongo-oplog');

var logger = {
  added: function() { console.log("added: "); console.log(arguments); },
  removed: function() { console.log("removed: "); console.log(arguments); },
  changed: function() { console.log("changed: "); console.log(arguments); },
  moved: function() { console.log("moved: "); console.log(arguments); },
};

var oplogUri = 'mongodb://127.0.0.1:27017/local';

var Observer = function(query, queryOptions, collection, options) {
  var self = this;
  const namespace = collection._collection.s.namespace;
  this._query = query;
  this._options = options;
  // this._options = logger;
  this._collection = collection;
  this._cache = [];

  var oplog = MongoOplog(oplogUri, { ns: namespace });
  oplog.on('insert', self._onInsertUpdate.bind(this));
  oplog.on('update', self._onInsertUpdate.bind(this));
  oplog.on('delete', self._onRemove.bind(this));

  oplog.tail().then(function () {
    self.loadInitial();
  });


  return {
    stop: function() {
      this._cache = null;
      oplog.stop();
      oplog.destroy();
    }
  }
};

var comparator = function (a, b) {
  return a._id === b._id
};

Observer.prototype.loadInitial = function() {
  var self = this;
  this._collection._getDocuments(this._query, function(err, result) {
    if(self._options.init) {
      self._cache = result;
      self._options.init(result);
    } else {
      var old = self._cache;
      self._cache = merge(old, result, _.defaults({
        comparatorId: function (a, b) {
          return a._id === b._id
        }
      }, self._options));
      //rewind cursor for next query...
    }
  });
};

Observer.prototype._onInsertUpdate = function (doc) {
  var self = this;
  var old = self._cache;
  var change = [doc.o]
  self._cache = merge(old, change, _.defaults({ comparatorId: comparator }, self._options));
};

Observer.prototype._onRemove = function (doc) {
  var self = this;
  var old = self._cache;
  var change = doc.o
  self._cache = merge(old, change, _.defaults({ comparatorId: comparator }, self._options));
};

module.exports = Observer;
