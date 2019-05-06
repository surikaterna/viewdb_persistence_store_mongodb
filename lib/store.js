var Promise = require('bluebird');
var Collection = require('./collection');
var MongoOplog = require('mongo-oplog');

var Store = function(mongodb, oplogEnabled) {
	this._mongodb = mongodb;
	this._collections = {};
	this._oplogEnabled = oplogEnabled;
	if (oplogEnabled) {
		var mongodbOplog = mongodb.db('local'); // for oplog observer
		this._oplog = MongoOplog(mongodbOplog);
	}
};

Store.prototype.open = function(callback) {
	var self = this;
	if (self._oplogEnabled) {
		return this._oplog.tail().then(function () {
			return Promise.resolve(self).nodeify(callback);
		});
	}
	return Promise.resolve(self).nodeify(callback);
};

Store.prototype.collection = function(collectionName, callback) {
	var coll = this._collections[collectionName];
	if(coll === undefined) {
		coll = new Collection(this._mongodb.collection(collectionName), this._oplog);
		this._collections[collectionName] = coll;
	}
	if(callback) {
		callback(coll);
	}
	return coll;
};

module.exports = Store;
