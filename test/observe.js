var should = require('should');


var MongoClient = require('mongodb').MongoClient;
var Server = require('mongodb').Server;

var ViewDb = require('viewdb');
var Store = require('../lib/store');

describe('Observe', function() {
	var _db = null;
	function getDb() {
		return _db;
	}
	function getVDb() {
		return new ViewDb(new Store(getDb()));
	}	

	before(function(done) {
		MongoClient.connect("mongodb://localhost:27017/db_test_suite", function(err, db) {
			_db = db;
			done();
		});
	});
	beforeEach(function(done) {
		_db.collection('dollhouse').drop(function() {
			done();
		});
	});

	afterEach(function(done) {
/*		_db.dropDatabase(function() {
			done();
		});
*/
		done();		
	});

	after(function(done) {
		_db.close(function() {
			done();
		});
	})	

	it('#observe with insert', function(done) {
		var store = getVDb();
		store.open().then(function() {
			var cursor = store.collection('dollhouse').find({});
			var handle = cursor.observe({
				added:function(x) {
					x._id.should.equal('echo');
					handle.stop();					
					done();
				}
			});
			store.collection('dollhouse').insert({_id:'echo'});
		});
	});
	it('#observe with query and insert', function(done) {
		var store = getVDb();
		store.open().then(function() {
			store.collection('dollhouse').insert({_id:'echo'});
			var cursor = store.collection('dollhouse').find({_id:'echo2'});
			var handle = cursor.observe({
				added:function(x) {
					x._id.should.equal('echo2');
					handle.stop();					
					done();
				}
			});
			store.collection('dollhouse').insert({_id:'echo2'});
		});
	});	
	it('#observe with query and update', function(done) {
		var store = getVDb();
		store.open().then(function() {
				var cursor = store.collection('dollhouse').find({_id:'echo'});
				var handle = cursor.observe({
					added:function(x) {
						x.age.should.equal(10);
						x._id.should.equal('echo');
					}, changed:function(o,n) {
						o.age.should.equal(10);
						n.age.should.equal(100);
						handle.stop();
						done();
					}
				});
			
			store.collection('dollhouse').insert({_id:'echo', age:10}, function(){
				store.collection('dollhouse').save({_id:'echo', age:100});
			});			
		});
	});	
})
