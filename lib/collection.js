var EventEmitter = require('events').EventEmitter;
var util = require('util');
var {forEach, isFunction, isArray} = require('lodash');

var Cursor = require('./cursor');
const { nodeify } = require('./utils');

var Collection = function (collection, oplogListener) {
  EventEmitter.call(this);
  this._collection = collection;
  this._oplogListener = oplogListener;
};

util.inherits(Collection, EventEmitter);

Collection.prototype.count = function () {
  return this._collection.count.apply(this._collection, arguments);
};

var wrapCallback = function (args, cb) {
  var realCb = args[args.length - 1];
  if (isFunction(realCb)) {
    args[args.length - 1] = function () {
      cb();
      realCb.apply(null, arguments);
    };
  } else {
    args = Array.prototype.slice.call(args);
    args.push(cb);
  }
  return args;
};

Collection.prototype.find = function (query, options) {
  var cursor = this._collection.find.apply(this._collection, arguments);
  return new Cursor(this, { query: query }, options, cursor, this._oplogListener);
};

Collection.prototype.findAndModify = function (query, sort, update, options, cb) {
  if (sort) {
    Object.assign(options, sort)
  }
  var self = this;
  return this._collection.findOneAndUpdate(query, update, options, function (err, doc,) {
    self.emit('change', { findAndModify: update });
    if (isFunction(cb)) {
      cb(err, doc);
    }
  })
}

Collection.prototype.updateMany = function (query, update, options, cb) {
  var self = this;
  var args = wrapCallback(arguments, function () {
    self.emit('change', { updateMany: update });
  });
  if (isFunction(options)) {
    cb = options;
  }
  return nodeify(this._collection.updateMany.apply(this._collection, args), cb);
};

Collection.prototype.updateOne = function (query, update, options, cb) {
  var self = this;
  var args = wrapCallback(arguments, function () {
    self.emit('change', { updateOne: update });
  });
  if (isFunction(options)) {
    cb = options;
  }
  return nodeify(this._collection.updateOne.apply(this._collection, args), cb);
};

Collection.prototype.remove = function (query, options) {
  console.warn('Deprecated: use deleteMany or deleteOne instead');
  var cb;
  var self = this;
  var args = wrapCallback(arguments, function () {
    self.emit('change', { remove: query });
  });
  if (isFunction(options)) {
    cb = options;
  } 
  return nodeify(this._collection.deleteMany.apply(this._collection, args), cb);
};

Collection.prototype.deleteMany = function (query, options) {
  var args = wrapCallback(arguments, function () {
    self.emit('change', { remove: query });
  });
  return this._collection.deleteMany.apply(this._collection, args);
}

Collection.prototype.deleteOne = function (query, options) {
  var args = wrapCallback(arguments, function () {
    self.emit('change', { remove: query });
  });
  return this._collection.deleteOne.apply(this._collection, args);
}

Collection.prototype.insert = function (docs, cb) {
  var self = this;
  // insertOne / insertMany modifies docs and adds inserted _id if applicable
  var onFulfilled = function () {
    self.emit("change", { insert: docs });
    if (isFunction(cb)) {
      cb(null, docs)
    }
  }
  var onRejected = function (err) {
    if (isFunction(cb)) {
      cb(err)
    }
  }
  var promise;
  if (isArray(docs)) {
    promise = this._collection.insertMany(docs).then(onFulfilled).catch(onRejected);
  } else {
    promise = this._collection.insertOne(docs).then(onFulfilled).catch(onRejected);
  }
  return promise;
};

Collection.prototype.save = function (docs, cb) {
  var self = this;
  if (!isArray(docs)) {
    docs = [docs]
  }
  const operations = [];
  forEach(docs, function (d) {
    if (!d._id) {
      operations.push({ insertOne: { document: d } })
    } else {
      operations.push({ replaceOne: { filter: { _id: d._id }, replacement: d, upsert: true } })
    }
  })
  // bulkWrite modifies docs and adds inserted _id if applicable
  this._collection.bulkWrite(operations)
    .then(function () {
      self.emit("change", { save: docs });
      if (isFunction(cb)) {
        cb(null, docs)
      }
    })
    .catch(function (err) {
      if (isFunction(cb)) {
        cb(err)
      }
    });
};

Collection.prototype.drop = function (cb) {
  var self = this;
  var args = wrapCallback(arguments, function () {
    self.emit('change', { drop: true });
  });
  return nodeify(this._collection.drop.apply(this._collection, args), cb);
};

Collection.prototype._getDocuments = function (queryObject, callback) {
  var query = queryObject.query || queryObject;
  var cursor = this._collection.find(query);
  if (queryObject.skip) {
    cursor.skip(queryObject.skip);
  }
  if (queryObject.limit) {
    cursor.limit(queryObject.limit);
  }
  if (queryObject.sort) {
    cursor.sort(queryObject.sort);
  }
  if (queryObject.project) {
    cursor.project(queryObject.project);
  }
  cursor.toArray().then(function (res, err) {
    if (err) {
      callback(err);
    } else {
      callback(null, res);
    }
  });
};

module.exports = Collection;
