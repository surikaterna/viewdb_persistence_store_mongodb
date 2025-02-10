const { MongoClient, ReadPreference } = require('mongodb');
const Store = require('../lib/store');

describe('mongodb_persistence', () => {
  const COLLECTION_NAME = 'collection';
  let _mongoClient;
  let _db;

  const getDb = () => _db;

  beforeAll(async () => {
    const mongoClient = await MongoClient.connect(global.__MONGO_URI__);
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

  describe('Collection', function () {
    it('#find with empty array should return 0 docs', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        store
          .collection(COLLECTION_NAME)
          .find({})
          .toArray(function (err, results) {
            expect(results).toHaveLength(0);
            done();
          });
      });
    });
    it('#find with setReadPreference', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        var cursor = store.collection(COLLECTION_NAME).find({});
        cursor.setReadPreference(ReadPreference.PRIMARY);
        cursor.toArray(function (err, results) {
          expect(results).toHaveLength(0);
          done();
        });
      });
    });
    it('#insert two documents with same key should throw', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        store.collection(COLLECTION_NAME).insert({ _id: 'echo' });
        store.collection(COLLECTION_NAME).insert({ _id: 'echo' }, function (err) {
          if (err) {
            done();
          } else {
            done(new Error('should have thrown unique constraint'));
          }
        });
      });
    });
    it('#update documents already existing', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        store.collection(COLLECTION_NAME).insert({ _id: 'existing' }, () => {
          store.collection(COLLECTION_NAME).save({ _id: 'existing', version: 2 }, () => {
            store
              .collection(COLLECTION_NAME)
              .find({})
              .toArray(function (err, results) {
                expect(results).toHaveLength(1);
                expect(results[0].version).toBe(2);
                done();
              });
          });
        });
      });
    });
    it('#update one document', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        store.collection(COLLECTION_NAME).insert({ _id: 'existing' }, () => {
          store.collection(COLLECTION_NAME).updateOne({ _id: 'existing' }, { $set: { _id: 'existing', name: 'john' } }, () => {
            store
              .collection(COLLECTION_NAME)
              .find({})
              .toArray(function (err, results) {
                expect(results).toHaveLength(1);
                expect(results[0].name).toBe('john');
                done();
              });
          });
        });
      });
    });

    it('#findAndModify upsert', async function () {
      const store = new Store(getDb());
      const filter = { _id: 'not-existing' };
      const sort = {};
      const update = { $setOnInsert: { test: 1 }, $push: { events: { event: 1, test: 2 }, references: { $each: [{ ref: 1 }, { ref: 2 }, { ref: 3 }] } } };
      const options = { upsert: true };
      await store.open()
      await store.collection(COLLECTION_NAME).findAndModify(filter, sort, update, options);
      const results = await store.collection(COLLECTION_NAME).find({}).toArray()
      expect(results).toHaveLength(1);
      expect(results[0]).toStrictEqual({
        _id: 'not-existing',
        events: [{ event: 1, test: 2 }],
        references: [{ ref: 1 }, { ref: 2 }, { ref: 3 }],
        test: 1
      });
    });

    it('#findAndModify modify', async function () {
      const store = new Store(getDb());
      const filter = { _id: 'not-existing' };
      const sort = {};
      const update = { $setOnInsert: { test: 1 }, $push: { events: { event: 1, test: 2 }, references: { $each: [{ ref: 1 }, { ref: 2 }, { ref: 3 }] } } };
      const options = { upsert: true };
      await store.open()
      await store.collection(COLLECTION_NAME).save({ _id: 'not-existing', test: 2, events: [{ event: 10, test: 3 }], references: [] });
      await store.collection(COLLECTION_NAME).findAndModify(filter, sort, update, options);
      const results = await store.collection(COLLECTION_NAME).find({}).toArray()
      expect(results).toHaveLength(1);
      expect(results[0]).toStrictEqual({
        _id: 'not-existing',
        events: [{ event: 10, test: 3 }, { event: 1, test: 2 }],
        references: [{ ref: 1 }, { ref: 2 }, { ref: 3 }],
        test: 2
      });
    });

    it('#update many documents', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        store.collection(COLLECTION_NAME).insert(
          [
            { _id: 'existing1', name: 'john' },
            { _id: 'existing2', name: 'john' }
          ],
          () => {
            store.collection(COLLECTION_NAME).updateMany({ name: 'john' }, { $set: { lastName: 'connor' } }, () => {
              store
                .collection(COLLECTION_NAME)
                .find({})
                .toArray(function (err, results) {
                  expect(results).toHaveLength(2);
                  expect(results[0]).toEqual({ _id: 'existing1', name: 'john', lastName: 'connor' });
                  expect(results[1]).toEqual({ _id: 'existing2', name: 'john', lastName: 'connor' });
                  done();
                });
            });
          }
        );
      });
    });
    it('#find {} should return single inserted document', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        store.collection(COLLECTION_NAME).insert({ _id: 'echo' }, function () {
          store
            .collection(COLLECTION_NAME)
            .find({})
            .toArray(function (err, results) {
              expect(results).toHaveLength(1);
              done();
            });
        });
      });
    });
    it('#find {} should return multiple inserted documents', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        // store.collection(COLLECTION_NAME).insert([{ _id: 'echo' }, { _id: 'sierra' }]);

        store.collection(COLLECTION_NAME).insert([{ _id: 'echo' }, { _id: 'sierra' }], function () {
          store
            .collection(COLLECTION_NAME)
            .find({})
            .toArray(function (err, results) {
              expect(results).toHaveLength(2);
              done();
            });
        });
      });
    });
    it('#find {_id:"echo"} should return correct document', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        store.collection(COLLECTION_NAME).insert({ _id: 'echo' });
        store.collection(COLLECTION_NAME).insert({ _id: 'sierra' }, function () {
          store
            .collection(COLLECTION_NAME)
            .find({ _id: 'echo' })
            .toArray(function (err, results) {
              expect(results).toHaveLength(1);
              expect(results[0]._id).toBe('echo');
              done();
            });
        });
      });
    });
    it('#find with complex key {"name.first":"echo"} should return correct document', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        store.collection(COLLECTION_NAME).insert({ _id: 'echo', name: { first: 'ECHO', last: 'TV' } });
        store.collection(COLLECTION_NAME).insert({ _id: 'sierra', name: { first: 'SIERRA', last: 'TV' } }, function () {
          store
            .collection(COLLECTION_NAME)
            .find({ 'name.first': 'ECHO' })
            .toArray(function (err, results) {
              expect(results).toHaveLength(1);
              expect(results[0]._id).toBe('echo');
              done();
            });
        });
      });
    });
    it('#find with project should return correct projection', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        store.collection(COLLECTION_NAME).insert({ _id: 'echo', name: { first: 'ECHO', last: 'TV' } }, () => {
          store.collection(COLLECTION_NAME).insert({ _id: 'sierra', name: { first: 'SIERRA', last: 'TV' } }, function () {
            store
              .collection(COLLECTION_NAME)
              .find({})
              .project({ _id: 1 })
              .toArray(function (err, results) {
                expect(results[0].name).toBeUndefined();
                expect(results[0]._id).toBe('echo');
                done();
              });
          });
        });
      });
    });
    it('#drop should remove all documents', function (done) {
      var store = new Store(getDb());
      store.open().then(function () {
        store.collection(COLLECTION_NAME).insert({ _id: 'echo' });
        store.collection(COLLECTION_NAME).drop(function () {
          store
            .collection(COLLECTION_NAME)
            .find({})
            .toArray(function (err, results) {
              expect(results).toHaveLength(0);
              done();
            });
        });
      });
    });
    it('#remove should remove one document', function (done) {
      var store = new Store(getDb());
      store.collection(COLLECTION_NAME).insert({ _id: 'echo', name: { first: 'ECHO', last: 'TV' } });
      store.collection(COLLECTION_NAME).insert({ _id: 'sierra', name: { first: 'SIERRA', last: 'TV' } }, function () {
        store.collection(COLLECTION_NAME).remove({ _id: 'echo' }, null,  function () {
          store
            .collection(COLLECTION_NAME)
            .find({ 'name.first': 'ECHO' })
            .toArray(function (err, results) {
              expect(results).toHaveLength(0);
              done();
            });
        });
      });
    });
    var populate = function (collection, id, cb) {
      collection.insert({ a: 'a', id: id }, function () {
        if (id === 9) {
          cb();
        } else {
          populate(collection, ++id, cb);
        }
      });
    };
    it('#skip/limit', function (done) {
      var store = new Store(getDb());
      var collection = store.collection(COLLECTION_NAME);
      populate(collection, 0, function () {
        collection
          .find({ a: 'a' })
          .skip(8)
          .limit(10)
          .toArray(function (err, res) {
            expect(res[1].id).toBe(9);
            expect(res).toHaveLength(2); // only 2 left after skipping 8/10
            done();
          });
      });
    });
    it('#count', function (done) {
      var store = new Store(getDb());
      var collection = store.collection(COLLECTION_NAME);
      populate(collection, 0, function () {
        collection
          .find({})
          .count(function (err, res) {
            expect(res).toBe(10);
            done();
          });
      });
    });
    it('#count should apply skip', function (done) {
      var store = new Store(getDb());
      var collection = store.collection(COLLECTION_NAME);
      populate(collection, 0, function () {
        collection
          .find({})
          .skip(8)
          .count(function (err, res) {
            expect(res).toBe(2);
            done();
          });
      });
    });
  });
})
;
