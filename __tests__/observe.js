var _ = require('lodash');
var MongoClient = require('mongodb').MongoClient;
var ViewDb = require('viewdb');
var Store = require('../lib/store');

describe('Observe', function () {
  const COLLECTION_NAME = 'observe';

  let _mongoClient;
  let _db;

  const getDb = () => _db;
  const getVDb = () => new ViewDb(new Store(getDb()));

  beforeAll(async () => {
    const mongoClient = await MongoClient.connect('mongodb://localhost:27017/db_test_suite', { useUnifiedTopology: true });
    const db = await mongoClient.db('db_test_suite');

    _mongoClient = mongoClient;
    _db = db;
  });

  beforeEach(async () => {
    try {
      await _db.collection(COLLECTION_NAME).drop();
    } catch (err) {
      // No-op
    }
  });

  afterAll(async () => {
    await _mongoClient.close();
  });

  it('#observe with query and update', function (done) {
    var store = getVDb();
    store.open().then(function () {
      var cursor = store.collection(COLLECTION_NAME).find({ _id: 'echo' });
      var handle = cursor.observe({
        added: function (x) {
          expect(x.age).toBe(10);
          expect(x._id).toBe('echo');
        },
        changed: function (asis, tobe) {
          expect(asis.age).toBe(10);
          expect(tobe.age).toBe(100);
          handle.stop();
          done();
        }
      });
      store.collection(COLLECTION_NAME).insert({ _id: 'echo', age: 10 }, function () {
        store.collection(COLLECTION_NAME).save({ _id: 'echo', age: 100 }, function () {});
      });
    });
  });
  it('#observe with insert', function (done) {
    var handle;
    var store = getVDb();
    store.open().then(function () {
      var collection = store.collection(COLLECTION_NAME);
      var cursor = collection.find({});
      handle = cursor.observe({
        added: function (x) {
          expect(x._id).toBe('echo');
          handle.stop();
          done();
        }
      });
      collection.insert({ _id: 'echo' });
    });
  });
  it('#observe with remove', function (done) {
    var realDone = _.after(2, done);
    var store = getVDb();
    store.open().then(function () {
      var cursor = store.collection(COLLECTION_NAME).find({});
      var handle = cursor.observe({
        added: function (x) {
          expect(x._id).toBe('echo');
          realDone();
        },
        removed: function () {
          handle.stop();
          realDone();
        }
      });
      var coll = store.collection(COLLECTION_NAME);
      coll.insert({ _id: 'echo' }, function () {
        coll.remove({ _id: 'echo' }, function () {});
      });
    });
  });
  it('#observe with query and insert', function (done) {
    var store = getVDb();
    store.open().then(function () {
      store.collection(COLLECTION_NAME).insert({ _id: 'echo1' }, function () {
        var cursor = store.collection(COLLECTION_NAME).find({ _id: 'echo2' });
        var handle = cursor.observe({
          added: function (x) {
            expect(x._id).toBe('echo2');
            done();
            handle.stop();
          }
        });
      });
      store.collection(COLLECTION_NAME).insert({ _id: 'echo4' }, function () {
        store.collection(COLLECTION_NAME).insert({ _id: 'echo2' });
      });
    });
  });
  it('#observe with query and skip', function (done) {
    var store = getVDb();
    store.open().then(function () {
      store.collection(COLLECTION_NAME).insert({ _id: 'echo' });
      store.collection(COLLECTION_NAME).insert({ _id: 'echo2' });
      store.collection(COLLECTION_NAME).insert({ _id: 'echo3' });
      var cursor = store.collection(COLLECTION_NAME).find({});
      var skip = 0;
      var handle;
      cursor.limit(1);
      var realDone = _.after(3, function () {
        cursor.toArray(function (err, res) {
          expect(res).toHaveLength(0);
          handle.stop();
          done();
        });
      });

      handle = cursor.observe({
        added: function () {
          cursor.skip(++skip);
          realDone();
        }
      });
    });
  });
});
