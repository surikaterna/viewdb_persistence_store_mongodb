var LoggerFactory = require('slf').LoggerFactory;
var slfDebug = require('slf-debug').default;

LoggerFactory.setFactory(slfDebug);

module.exports = require('./store');
