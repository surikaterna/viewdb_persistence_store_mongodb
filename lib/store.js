var Promise = require('bluebird');
var Collection = require('./collection');
var OplogListener = require('./oplog_listener');
var _ = require('lodash');

var Store = function(mongodb, oplogEnabled, oplogListener, mongodbOplog) {
	this._mongodb = mongodb;
	this._mongodbOplog = mongodbOplog;
	this._oplogListeners = {};
	this._collections = {};
	this._oplogListener = oplogListener || OplogListener;
	this._oplogEnabled = oplogEnabled;
};

Store.prototype.open = function(callback) {
	var self = this;
	return Promise.resolve(self).nodeify(callback);
};

Store.prototype.collection = function(collectionName, callback) {
	var coll = this._collections[collectionName];
	if(coll === undefined) {
		if (this._oplogEnabled) {
			var dbName = _.get(this._mongodb, 'databaseName');
			var namespaceFilter;
			if (dbName) {
				namespaceFilter = dbName + '.' + collectionName;
			}
			this._oplogListeners[collectionName] = new this._oplogListener(this._mongodb, namespaceFilter, this._mongodbOplog, collectionName)
		}
		coll = new Collection(this._mongodb.collection(collectionName), this._oplogListeners[collectionName]);
		this._collections[collectionName] = coll;
	}
	if(callback) {
		callback(coll);
	}
	return coll;
};

module.exports = Store;
