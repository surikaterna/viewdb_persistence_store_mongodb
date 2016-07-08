var should = require('should');
var MongoClient = require('mongodb').MongoClient;
var Server = require('mongodb').Server;

var Store = require('../lib/store');

describe('mongodb_persistence', function () {
	var _db = null;
	function getDb() {
		return _db;
	}
	before(function (done) {
		MongoClient.connect("mongodb://localhost:27017/db_test_suite", function (err, db) {
			_db = db;
			done();
		});
	});
	beforeEach(function (done) {
		_db.collection('dollhouse').drop(function () {
			done();
		});
	});

	afterEach(function (done) {
		/*		_db.dropDatabase(function() {
					done();
				});
		*/
		done();
	});

	after(function (done) {
		_db.close(function () {
			done();
		});
	})
	describe('Collection', function () {
		it('#find with empty array should return 0 docs', function (done) {
			var store = new Store(getDb());
			store.open().then(function () {
				store.collection('dollhouse').find({}).toArray(function (err, results) {
					results.length.should.equal(0);
					done();
				});
			});
		});
		it('#insert two documents with same key should throw', function (done) {
			var store = new Store(getDb());
			store.open().then(function () {
				store.collection('dollhouse').insert({ _id: 'echo' });
				store.collection('dollhouse').insert({ _id: 'echo' }, function (err, result) {
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
				store.collection('dollhouse').insert({ _id: 'echo' });
				store.collection('dollhouse').save({ _id: 'echo', version: 2 });
				store.collection('dollhouse').find({}).toArray(function (err, results) {
					results.length.should.equal(1);
					results[0].version.should.equal(2);
					done();
				});
			});
		});
		it('#find {} should return single inserted document', function (done) {
			var store = new Store(getDb());
			store.open().then(function () {
				store.collection('dollhouse').insert({ _id: 'echo' });
				store.collection('dollhouse').find({}).toArray(function (err, results) {
					results.length.should.equal(1);
					done();
				});
			});
		});
		it('#find {} should return multiple inserted documents', function (done) {
			var store = new Store(getDb());
			store.open().then(function () {
				store.collection('dollhouse').insert({ _id: 'echo' });
				store.collection('dollhouse').insert({ _id: 'sierra' });
				store.collection('dollhouse').find({}).toArray(function (err, results) {
					results.length.should.equal(2);
					done();
				});
			});
		});
		it('#find {_id:"echo"} should return correct document', function (done) {
			var store = new Store(getDb());
			store.open().then(function () {
				store.collection('dollhouse').insert({ _id: 'echo' });
				store.collection('dollhouse').insert({ _id: 'sierra' });
				store.collection('dollhouse').find({ _id: 'echo' }).toArray(function (err, results) {
					results.length.should.equal(1);
					results[0]._id.should.equal('echo');
					done();
				});
			});
		});
		it('#find with complex key {"name.first":"echo"} should return correct document', function (done) {
			var store = new Store(getDb());
			store.open().then(function () {
				store.collection('dollhouse').insert({ _id: 'echo', name: { first: 'ECHO', last: "TV" } });
				store.collection('dollhouse').insert({ _id: 'sierra', name: { first: 'SIERRA', last: "TV" } });
				store.collection('dollhouse').find({ "name.first": 'ECHO' }).toArray(function (err, results) {
					results.length.should.equal(1);
					results[0]._id.should.equal('echo');
					done();
				});
			});
		});
		it('#drop should remove all documents', function (done) {
			var store = new Store(getDb());
			store.open().then(function () {
				store.collection('dollhouse').insert({ _id: 'echo' });
				store.collection('dollhouse').drop();

				store.collection('dollhouse').find({}).toArray(function (err, results) {
					results.length.should.equal(0);
					done();
				});
			});
		});
		it('#remove should remove one document', function (done) {
			var store = new Store(getDb());
			store.collection('dollhouse').insert({ _id: 'echo', name: { first: 'ECHO', last: "TV" } });
			store.collection('dollhouse').insert({ _id: 'sierra', name: { first: 'SIERRA', last: "TV" } });
			store.collection('dollhouse').remove({ _id: 'echo', name: { first: 'ECHO', last: "TV" } });

			store.collection('dollhouse').find({ 'name.first': 'ECHO'}).toArray(function(err, results) {
				results.length.should.equal(0);
				done();
			});
		})
	})
})