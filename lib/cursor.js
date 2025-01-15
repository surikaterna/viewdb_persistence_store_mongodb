var Observer = require('./observe');
var {nodeify} = require('./utils');

var Cursor = function (collection, query, options, cursor, oplogListener) {
  this._query = query;
  this._queryOptions = options || {};
  this._cursor = cursor;
  this._oplogListener = oplogListener;
  this._collection = collection;
};

Cursor.prototype.each = function () {
  return this._cursor.each.apply(this._cursor, arguments);
};

Cursor.prototype.setReadPreference = function () {
   this._cursor.withReadPreference.apply(this._cursor, arguments);
   return this;
};

Cursor.prototype.count = function (callback) {
  return nodeify(this._cursor.count.apply(this._cursor, arguments), callback);
};

Cursor.prototype.project = function (project) {
  this._cursor.project.apply(this._cursor, arguments);
  this._queryOptions.project = project;
  return this;
};

Cursor.prototype.toArray = function (callback) {
  return nodeify(this._cursor.toArray.apply(this._cursor, arguments), callback);
};

Cursor.prototype.observe = function (options) {
  return new Observer(this._query, this._queryOptions, this._collection, options, this._oplogListener);
};

Cursor.prototype.skip = function (skip) {
  this._cursor.skip.apply(this._cursor, arguments);
  this._queryOptions.skip = skip;
  this._refresh();
  return this;
};

Cursor.prototype.limit = function (limit) {
  this._queryOptions.limit = limit;
  this._cursor.limit.apply(this._cursor, arguments);
  this._refresh();
  return this;
};

Cursor.prototype.sort = function (sort) {
  this._queryOptions.sort = sort;
  this._cursor.sort.apply(this._cursor, arguments);
  this._refresh();
  return this;
};

Cursor.prototype._refresh = function () {
  this._collection.emit('change', {});
};

Cursor.prototype.rewind = function () {
  return this._cursor.rewind.apply(this._cursor, arguments);
};

module.exports = Cursor;
