var should = require('should');
var _ = require('lodash');
var MongoClient = require('mongodb').MongoClient;
var ViewDb = require('viewdb');
var Store = require('../lib/store');

describe('Oplog Observe', function () {
  var _db, client = null;
  function getDb() {
    return _db;
  }
  function getVDb() {
    return new ViewDb(new Store(getDb(), true));
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
    _db.collection('dollhouse').remove({}, done)
  });
  afterEach(function (done) {
    done();
  });
  after(function (done) {
    _db.dropDatabase(function () {
      client.close(function () {
        done();
      });
    });
  });
  it('#oplog observe with query and update', function (done) {
    var store = getVDb();
    store.open().then(function () {
      var cursor = store.collection('dollhouse').find({ _id: 'echo' });
      var handle = cursor.observe({
        oplog: true,
        added: function (x) {
          x.age.should.equal(10);
          x._id.should.equal('echo');
        },
        changed: function (asis, tobe) {
          tobe.age.should.equal(100);
          tobe.someOtherProp.should.equal('yes');
          handle.stop();
          done();
        }
      });
      store.collection('dollhouse').insert({ _id: 'echo', age: 10, someOtherProp: 'yes' }, function () {
        store.collection('dollhouse').save({ _id: 'echo', age: 100, someOtherProp: 'yes' }, function () {
        });
      });
    });
  });
  it('#oplog observe with query and update falling outside of query', function (done) {
    var store = getVDb();
    store.open().then(function () {
      var cursor = store.collection('dollhouse').find({ _id: 'echo', age: { $gte: 10 } });
      var handle = cursor.observe({
        oplog: true,
        added: function (x) {
          x.age.should.equal(10);
          x._id.should.equal('echo');
        },
        changed: function () {
          done(new Error('Should not be part of query'));
        },
        removed: function (x) {
          x._id.should.equal('echo');
          handle.stop();
          done();
        }
      });
      store.collection('dollhouse').insert({ _id: 'echo', age: 10, someOtherProp: 'yes' }, function () {
        store.collection('dollhouse').save({ _id: 'echo', age: 5, someOtherProp: 'yes' }, function () {
        });
      });
    });
  });
  it('#oplog observe with insert', function (done) {
    var handle;
    var store = getVDb();
    store.open().then(function () {
      var collection = store.collection('dollhouse');
      var cursor = collection.find({});
      handle = cursor.observe({
        oplog: true,
        added: function (x) {
          x._id.should.equal('echo');
          handle.stop();
          done();
        }
      });
      setTimeout(function () {
        collection.insert({ _id: 'echo' });
      }, 10)
    });
  });
  it('#oplog observe with findAndModify', function (done) {
    var handle;
    var store = getVDb();
    store.open().then(function () {
      var collection = store.collection('dollhouse');
      var cursor = collection.find({});
      handle = cursor.observe({
        oplog: true,
        added: function (x) {
          x._id.should.equal('echo');
          x.someOldProp.should.equal('cool');
        },
        changed: function (asis, tobe, index) {
          tobe._id.should.equal('echo');
          tobe.newProp.should.equal('yes');
          tobe.someOldProp.should.equal('cool');
          handle.stop();
          done();
        }
      });
      collection.insert({ _id: 'echo', someOldProp: 'cool' }, () => {
        collection.findAndModify({ _id: 'echo' }, null, { $set: { newProp: 'yes' } });
      });
    });
  })
  it('#oplog observe with query and update falling outside of query with findAndModify ', function (done) {
    var handle;
    var store = getVDb();
    store.open().then(function () {
      var collection = store.collection('dollhouse');
      var cursor = collection.find({ age: { $gte: 10 } });
      handle = cursor.observe({
        oplog: true,
        added: function (x) {
          x._id.should.equal('echo');
          x.age.should.equal(10);
        },
        changed: function (a, t) {
          done(new Error('Should not be part of query'));
        },
        removed: function (x) {
          x._id.should.equal('echo');
          x.age.should.equal(5);
          handle.stop();
          done();
        }
      });
      collection.insert({ _id: 'echo', age: 10 }, function () {
        collection.findAndModify({ _id: 'echo' }, null, { $set: { age: 5 } });
      });
    });
  })
  it('#oplog observe with findAndModify upsert', function (done) {
    var handle;
    var store = getVDb();
    store.open().then(function () {
      var collection = store.collection('dollhouse');
      var cursor = collection.find({});
      handle = cursor.observe({
        oplog: true,
        added: function (x) {
          x._id.should.equal('echo');
          x.newProp.should.equal('yes');
          x.moarComplex.yes.sir.should.equal(true);
          handle.stop();
          done();
        }
      });
      setTimeout(function () {
        var update = { $set: { newProp: 'yes', moarComplex: { yes: { sir: true } } } };
        var options = { upsert: true };
        collection.findAndModify({ _id: 'echo' }, null, update, options);
      }, 10);
    });
  });
  it('#oplog observe with remove', function (done) {
    var realDone = _.after(2, done);
    var store = getVDb();
    store.open().then(function () {
      var cursor = store.collection('dollhouse').find({});
      var handle = cursor.observe({
        oplog: true,
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
  it('#oplog observe with query and insert', function (done) {
    var store = getVDb();
    store.open().then(function () {
      store.collection('dollhouse').insert({ _id: 'echo1' }, function () {
        var cursor = store.collection('dollhouse').find({ _id: 'echo2' });
        var handle = cursor.observe({
          oplog: true,
          added: function (x) {
            x._id.should.equal('echo2');
            handle.stop();
            done();
          }
        });
      });
      store.collection('dollhouse').insert({ _id: 'echo4' }, function () {
        setTimeout(function () {
          store.collection('dollhouse').insert({ _id: 'echo2' });
        }, 50)
      });
    });
  });
  it('#oplog observe with query and skip', function (done) {
    var store = getVDb();
    store.open().then(function () {
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
        oplog: true,
        added: function (x) {
          cursor.skip(++skip);
          realDone();
        }
      });
      store.collection('dollhouse').insert([{ _id: 'echo' }, { _id: 'echo2' }, { _id: 'echo3' }]);
    });
  })
});
