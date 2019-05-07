var Promise = require('bluebird');
var Collection = require('./collection');
var OplogListener = require('./oplog_listener');

var Store = function(mongodb, oplogEnabled) {
	this._mongodb = mongodb;
	this._collections = {};
	this._oplogListener = undefined;
	if (oplogEnabled) {
		this._oplogListener = new OplogListener(mongodb)
	}
};

Store.prototype.open = function(callback) {
	var self = this;
	return Promise.resolve(self).nodeify(callback);
};

Store.prototype.collection = function(collectionName, callback) {
	var coll = this._collections[collectionName];
	if(coll === undefined) {
		coll = new Collection(this._mongodb.collection(collectionName), this._oplogListener);
		this._collections[collectionName] = coll;
	}
	if(callback) {
		callback(coll);
	}
	return coll;
};

module.exports = Store;
