var MongoOplog = require('mongo-oplog');
var _ = require('lodash');

var OplogListener = function (mongodb) {
  var self = this;
  this._mongodb = mongodb;
  var mongodbOplog = mongodb.db('local');
  this._oplog = MongoOplog(mongodbOplog);
  this._oplog.on('error', self._onError.bind(self));
  this._isTailing = false;
  this._listeners = []
};

OplogListener.prototype._onError = function (err) {
  console.log('Error in oplog listener', _.get(err, 'message'));
  var self = this;
  var retryTimeout = 1000 + Math.floor(Math.random() * 250);
  setTimeout(function () {
    var mongodbOplog = self._mongodb.db('local');
    self._oplog = MongoOplog(mongodbOplog);
    self._oplog.on('error', self._onError.bind(self));
    self._isTailing = false;
    self.start().then(function () {
      console.log('[mongo-oplog] - Reconnected. Restoring ' + self._listeners.length + ' listeners');
      _.forEach(self._listeners, function (listener) {
        var filter = self._oplog.filter(listener.namespace);
        filter.on('op', listener.onOperation, listener.context);
      })
    })
  }, retryTimeout);
};

OplogListener.prototype.start = function () {
  if (!this._isTailing) {
    console.log('[mongo-oplog] - connecting to oplog stream');
    this._isTailing = true;
    return this._oplog.tail().then(function (stream) {
      if (stream) {
        console.log('[mongo-oplog] - connected to oplog stream');
      }
    });
  }
  return Promise.resolve();
};

OplogListener.prototype.listen = function (namespace, onOperation, context) {
  var self = this;
  var listenerId = '_' + Math.random().toString(36).substr(2, 9);
  var filter = this._oplog.filter(namespace);
  filter.on('op', onOperation, context);

  this.start().then(function () {
    self._listeners.push({ listenerId: listenerId, namespace: namespace, onOperation: onOperation, context: context })
  });
  // stop
  return function () {
    _.remove(self._listeners, { listenerId: listenerId })
    filter.destroy();
  }
};

module.exports = OplogListener;
