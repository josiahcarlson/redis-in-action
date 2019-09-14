#!/usr/local/bin/node
// Code adapted from http://ronderksen.nl/2012/05/03/debugging-mocha-tests-in-webstorm/

var Mocha = require('mocha'),
	path = require('path'),
	fs = require('fs');

var mocha = new Mocha({
	reporter: 'dot',
	ui: 'bdd',
	timeout: 999999
});

var testDir = './ch01/';

fs.readdir(testDir, function (err, files) {

	if (err) {
		console.log(err);
		return;
	}

	files.forEach(function (file) {
		if (file === 'mocha-test.js') {
			mocha.addFile(testDir + file);
		}
	});

	var runner = mocha.run(function () {
		console.log('finished');
	});

	runner.on('pass', function (test) {
//		console.log('... %s passed', test.title);
	});

	runner.on('fail', function (test) {
		console.log('... %s failed', test.title);
	});
});