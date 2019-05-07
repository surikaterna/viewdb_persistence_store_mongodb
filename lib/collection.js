var EventEmitter = require('events').EventEmitter;
var util = require('util');
var _ = require('lodash');

var Cursor = require('./cursor');

var Collection = function(collection, oplogListener) {
  EventEmitter.call(this);
  this._collection = collection;
  this._oplogListener = oplogListener;
}

util.inherits(Collection, EventEmitter);

Collection.prototype.count = function() {
  return this._collection.count.apply(this._collection, arguments);
}

var wrapCallback = function (args, cb) {
  var realCb = args[args.length - 1];
  if (_.isFunction(realCb)) {
    args[args.length - 1] = function () {
      cb();
      realCb.apply(null, arguments);
    }
  } else {
    args = Array.prototype.slice.call(args);
    args.push(cb);
  }
  return args;
}

Collection.prototype.find = function(query, options) {
  var cursor = this._collection.find.apply(this._collection, arguments)
  return new Cursor(this, {query: query}, options, cursor, this._oplogListener);
};

Collection.prototype.findAndModify = function(query, sort, update, options, cb) {
  var self = this;
  var args = wrapCallback(arguments, function () {
    self.emit("change", {findAndModify: update});
  });
  this._collection.findAndModify.apply(this._collection, args)
}

Collection.prototype.remove = function(query, options) {
  var self = this;
  var args = wrapCallback(arguments, function () {
    self.emit("change", {remove:query});
  });
  this._collection.remove.apply(this._collection, args);
}

Collection.prototype.insert = function(docs) {
  var self = this;
  var args = wrapCallback(arguments, function () {
    self.emit("change", {insert: docs});
  });
  this._collection.insert.apply(this._collection, args);
}

Collection.prototype.save = function(docs) {
  var self = this;
  var args = wrapCallback(arguments, function () {
    self.emit("change", {save: docs});
  });
  this._collection.save.apply(this._collection, args);
}

Collection.prototype.drop = function () {
  var self = this;
  var args = wrapCallback(arguments, function () {
    self.emit("change", {drop:true});
  });
  this._collection.drop.apply(this._collection, args);
}

Collection.prototype._getDocuments = function(queryObject, callback) {
  var query = queryObject.query || queryObject;
  var cursor = this._collection.find(query);
  if (queryObject.skip) {
    cursor.skip(queryObject.skip)
  }
  if (queryObject.limit) {
    cursor.limit(queryObject.limit);
  }
  if (queryObject.sort) {
    cursor.sort(queryObject.sort);
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
