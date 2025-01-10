var _ = require('lodash');

function _includeKey(key) {
  return key === '1' || key === true || key === 1;
}

function _excludeKey(key) {
  return key === '0' || key === false || key === 0;
}

function _projectLayer(document, projectObject) {
  var projectedLayer = {};
  var deletionKeys = [];

  _.forEach(projectObject, function (value, key) {
    if (_excludeKey(value)) {
      deletionKeys.push(key);
    }
  });

  if (deletionKeys.length > 0) {
    projectedLayer = _.omit(document, deletionKeys);
  }

  _.forEach(projectObject, function (value, key) {
    if (_.isArray(document[key])) {
      projectedLayer[key] = document[key].map(function (arrayValue) {
        return _projectLayer(arrayValue, projectObject[key]);
      });
    } else if (_.isObject(value)) {
      projectedLayer[key] = _projectLayer(document[key], value);
    } else if (_includeKey(value)) {
      projectedLayer[key] = document[key];
    }
  });

  return projectedLayer;
}

var nextTick;
if (typeof setImmediate === 'function') {
  nextTick = setImmediate
} else if (typeof process === 'object' && process && process.nextTick) {
  nextTick = process.nextTick
} else {
  nextTick = function (cb) {
    setTimeout(cb, 0)
  }
}

function nodeify(promise, cb) {
  if (typeof cb !== 'function') return promise;
  return promise
    .then(function (res) {
      nextTick(function () {
        cb(null, res);
      });
    })
    .catch(function (err) {
      nextTick(function () {
        cb(err);
      });
    });
}

/**
 * Performs a MongoDb $project
 * @param {object} document MongoDb like document
 * @param {object} projectObject MongoDb like project object
 * @returns projected version of the document
 */
function projectDocument(document, projectObject) {
  return _projectLayer(document, projectObject);
}

module.exports = {
  projectDocument,
  nodeify
};
