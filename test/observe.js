var should = require('should');
var _ = require('lodash');
var MongoClient = require('mongodb').MongoClient;
var ViewDb = require('viewdb');
var Store = require('../lib/store');

describe('Observe', function () {
  var _db, client = null;
  function getDb() {
    return _db;
  }
  function getVDb() {
    return new ViewDb(new Store(getDb()));
  }
  before(function (done) {
    const url = "mongodb://localhost:27017";
    client = new MongoClient(url, {});
    client.connect(function (err) {
      _db = client.db('db_test_suite');
      done();
    });
  });
  beforeEach(function (done) {
    _db.collection('dollhouse').drop(function () {
      done();
    });
  });
  afterEach(function (done) {
		_db.dropDatabase(function() {
		 done();
		 });
    // done();
  });
  after(function (done) {
    client.close(function () {
      done();
    });
  });
  it('#observe with query and update', function (done) {
    var store = getVDb();
    store.open().then(function () {
      var cursor = store.collection('dollhouse').find({ _id: 'echo' });
      var handle = cursor.observe({
        added: function (x) {
          x.age.should.equal(10);
          x._id.should.equal('echo');
        },
        changed: function (asis, tobe) {
          asis.age.should.equal(10);
          tobe.age.should.equal(100);
          handle.stop();
          done();
        }
      });
      store.collection('dollhouse').insert({ _id: 'echo', age: 10 }, function () {
        store.collection('dollhouse').save({ _id: 'echo', age: 100 }, function () {});
      });
    });
  });
  it('#observe with insert', function (done) {
    var handle;
    var store = getVDb();
    store.open().then(function () {
      var collection = store.collection('dollhouse');
      var cursor = collection.find({});
      handle = cursor.observe({
        added: function (x) {
          x._id.should.equal('echo');
          handle.stop();
          done();
        }
      });
      collection.insert({ _id: 'echo' });
    });
  })
  it('#observe with remove', function (done) {
    var realDone = _.after(2, done);
    var store = getVDb();
    store.open().then(function () {
      var cursor = store.collection('dollhouse').find({});
      var handle = cursor.observe({
        added: function (x) {
          x._id.should.equal('echo');
          realDone();
        },
        removed: function (x) {
          handle.stop();
          realDone();
        }
      });
      var coll = store.collection('dollhouse');
      coll.insert({ _id: 'echo' }, function () {
        coll.remove({ _id: 'echo' });
      });
    });
  })
  it('#observe with query and insert', function (done) {
    var store = getVDb();
    store.open().then(function () {
      store.collection('dollhouse').insert({ _id: 'echo1' }, function () {
        var cursor = store.collection('dollhouse').find({ _id: 'echo2' });
        var handle = cursor.observe({
          added: function (x) {
            x._id.should.equal('echo2');
            done();
            handle.stop();
          }
        });
      });
      store.collection('dollhouse').insert({ _id: 'echo4' }, function () {
        store.collection('dollhouse').insert({ _id: 'echo2' });
      });
    });
  })
  it('#observe with query and skip', function (done) {
    var store = getVDb();
    store.open().then(function () {
      store.collection('dollhouse').insert({ _id: 'echo' });
      store.collection('dollhouse').insert({ _id: 'echo2' });
      store.collection('dollhouse').insert({ _id: 'echo3' });
      var cursor = store.collection('dollhouse').find({});
      var skip = 0;
      var handle;
      cursor.limit(1);
      var realDone = _.after(3, function () {
        cursor.toArray(function (err, res) {
          res.length.should.equal(0);
          handle.stop();
          done();
        })
      });

      handle = cursor.observe({
        added: function (x) {
          cursor.skip(++skip);
          realDone();
        }
      });
    });
  })
});
