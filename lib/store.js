var Promise = require('bluebird');
var Collection = require('./collection');
var Store = function(mongodb) {
	this._mongodb = mongodb;
	this._collections = {};
}

Store.prototype.open = function(callback) {
	return Promise.resolve(this).nodeify(callback);
}

Store.prototype.collection = function(collectionName, callback) {
	var coll = this._collections[collectionName];
	if(coll === undefined) {
		coll = new Collection(this._mongodb.collection(collectionName));
		this._collections[collectionName] = coll;
	}
	if(callback) {
		callback(coll);
	}
	return coll;
};

module.exports = Store;