var EventEmitter = require('events').EventEmitter;
var util = require('util');

var Cursor = require('./cursor');

var Collection = function(collection) {
	EventEmitter.call(this);
	this._collection = collection;
}

util.inherits(Collection, EventEmitter);

Collection.prototype.count = function() {
	return this._collection.count.apply(this._collection, arguments);
}

Collection.prototype.find = function(query, options) {
	var cursor = this._collection.find.apply(this._collection, arguments)
	return new Cursor(this, query, options, cursor);
}

Collection.prototype.remove = function(query, options) {
	this._collection.remove.apply(this._collection, arguments);
	this.emit("change", {});
}

Collection.prototype.insert = function(docs) {
	this._collection.insert.apply(this._collection, arguments);
	this.emit("change", docs);
}

Collection.prototype.save = function(docs) {
	this._collection.save.apply(this._collection, arguments);
	this.emit("change", docs);
}

Collection.prototype.drop = function() {
	this._collection.drop.apply(this._collection, arguments);
	this.emit("change", {});
}

module.exports = Collection;