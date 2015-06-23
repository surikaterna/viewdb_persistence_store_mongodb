var Promise = require('bluebird');

var Store = function(mongodb) {
	this._mongodb = mongodb;
}

Store.prototype.open = function(callback) {
	return Promise.resolve(this).nodeify(callback);
}

Store.prototype.collection = function(collectionName, callback) {
	if(callback) {
		return this._mongodb.collection(collectionName, callback);
	} else {
		return this._mongodb.collection(collectionName);
	}
};

module.exports = Store;