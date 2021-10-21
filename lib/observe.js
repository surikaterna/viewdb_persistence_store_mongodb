var _ = require('lodash');
var merge = require('viewdb').merge;
var LegacyObserver = require('viewdb').Observer;
var Kuery = require('kuery');

var logger = {
  added: function () {
    console.log("added: ");
    console.log(arguments);
  },
  removed: function () {
    console.log("removed: ");
    console.log(arguments);
  },
  changed: function () {
    console.log("changed: ");
    console.log(arguments);
  },
  moved: function () {
    console.log("moved: ");
    console.log(arguments);
  },
};

function projectObjectToPaths(object) {
  var includePaths = ['_id'];
  var removePaths = [];

  function flatten(obj, prefix) {
    _.forEach(obj, (value, key) => {
      if (_.isObject(value)) {
        flatten(value, `${prefix}${key}.`)
      } else {
        var path = `${prefix}${key}`
        if (value === '1' || value === true || value === 1) {
          includePaths.push(path);
        } else if (value === '0' || value === false || value === 0) {
          removePaths.push(path);
        }
      }
    })
  }

  flatten(object, '')

  return { includePaths, removePaths };
}

var Observer = function (query, queryOptions, collection, options, oplogListener) {
  var self = this;
  self._queryOptions = queryOptions;

  if (queryOptions.project) {
    var paths = projectObjectToPaths(this._queryOptions.project);
    this._compiledProjectPaths = paths;
  }

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

  self.loadInitial(function () {
    self.listener = oplogListener.listen(namespace, self._onOperation, self);
  });

  var dispose = function () {
    if (self.listener) {
      self.listener.dispose();
    }
    self._cache = null;
    return Promise.resolve()
  };
  return {
    stop: dispose,
    dispose: dispose
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
  if (!this._cache) {
    console.log('WARN: Got oplog event for document but cache was already disposed');
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
      console.log('WARN: Unhandled operation: ', doc.op);
      break;
  }
};

Observer.prototype._checkKuery = function (coll) {
  var q = new Kuery(this._query.query);
  var res = q.find(coll);
  return res && res.length > 0;
};

Observer.prototype._projectDocument = function (document) {
  var includePaths = this._compiledProjectPaths.includePaths;
  var removePaths = this._compiledProjectPaths.removePaths;

  var projectedDocument = document;
  if (includePaths.length > 0) {
   projectedDocument =  _.pick(projectedDocument, includePaths);
  }

  if (removePaths.length > 0) {
    projectedDocument = _.omit(projectedDocument, removePaths);
  }

  return projectedDocument;
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

      if (this._compiledProjectPaths) {
        document = this._projectDocument(doc.o);
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
