/*
PNY_TEST_1_DBO    
------------------
LXgfEdV5NZMMXJFeR

PNY_TEST_1_APP    
------------------
T6aUtq8PqV6OPDWFh

 */

var oracledb = require('oracledb');
var strLength = require('node-mb-string-size');
var stream = require('stream');
var fs = require('fs');
var xml2js = require('xml2js');
oracledb.autoCommit = true;
var debugConnectionQueries = false;
var idCounter = 0;
var connection;
var dbConfig = {
	user: "PNY_TEST_1_DBO",
	password: "LXgfEdV5NZMMXJFeR",
	connectString: "localhost/XE"
}

if (process.env.timeout) {
	jasmine.DEFAULT_TIMEOUT_INTERVAL = process.env.timeout;
} else {
	jasmine.DEFAULT_TIMEOUT_INTERVAL = 5*60*1000; // 5 minutes
}

function getNewId() {
	return idCounter++;
}

function createLargeFile(size) {
	var filePath = __dirname + "/" + size + "mb.txt";
	var str = randomString(100000)
	fs.writeFileSync(filePath, str, "utf8");
	for (var i = 1; i < size * 10; i++) {
		fs.appendFileSync(filePath, str);
	}
	console.log("Finished creating large file")
	var fileSizeInBytes = fs.statSync(filePath).size;
	console.log("Create file size " + formatBytes(fileSizeInBytes));

}

if (process.env.largefile) {
	createLargeFile(100);
	createLargeFile(1000);
	process.exit(0);
}

function formatBytes(bytes, decimals) {
	if (bytes == 0) return '0 Bytes';
	var k = 1000,
		dm = decimals || 2,
		sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
		i = Math.floor(Math.log(bytes) / Math.log(k));
	return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function streamToString(stream) {
	return new Promise(function(resolve, reject) {
		var chunks = [];
		stream.on('data', function(chunk) {
			chunks.push(chunk);
		});
		stream.on('end', function() {
			resolve(chunks.join(''));
		});
	})
}

function randomString(cloblength) {
	cloblength = cloblength || 47000;

	var random = "";
	for (var i = 0; i < cloblength; i++) {
		random += "a";
	}
	return random;
}

function doQuery(query, args, ignoreErr) {
	return new Promise(function(resolve, reject) {
		if (debugConnectionQueries) console.log("EXECUTING: " + query)
		connection.execute(
			query,
			args,
			function(err, result) {
				if (debugConnectionQueries) console.log({
					"err": err,
					"result": result
				})
				if (ignoreErr) {
					return resolve({});
				}
				if (err) {
					reject(err.message);
				}
				resolve(result);
			});
	})
}

function doBufferedInsertQuery(query, args) {
	return new Promise(function(resolve, reject) {
		console.log("doBufferedInsertQuery")
		if (debugConnectionQueries) console.log("EXECUTING: " + query)
		connection.execute(
			query, {
				lobbv: {
					type: oracledb.CLOB,
					dir: oracledb.BIND_OUT
				}
			}, {
				autoCommit: false
			},
			function(err, result) {
				if (err) return reject(err)
				var lob = result.outBinds.lobbv[0];
				lob.on('error', function(err) {
					console.log("lob.on 'error' event");
					console.error(err);
				});

				if (args.file) {
					console.log('Reading from ' + args.file);
					var inStream = fs.createReadStream(args.file);

					inStream.on(
						'end',
						function() {
							console.log("Stream finished")
							connection.commit(
								function(err) {
									if (err) {
										console.error(err);
										reject(err.message);
									} else {
										resolve();
									}
								});
						});
					inStream.on(
						'error',
						function(err) {
							console.log("inStream.on 'error' event");
							console.error(err);
							reject(err)
						});
					inStream.pipe(lob);
				} else {
					var bufferStream = new stream.PassThrough();
					bufferStream.end(new Buffer(args.data));
					bufferStream.pipe(lob)
					bufferStream.on('end', function() {
						connection.commit(
							function(err) {
								if (err)
									reject(err.message);
								else
									resolve();
							});
					});
				}

			});
	})
}

function doRelease(connection) {
	return new Promise(function(resolve, reject) {
		connection.close(
			function(err) {
				resolve(err);
			});
	})
}

function connect() {
	return new Promise(function(resolve, reject) {
		// Get a non-pooled connection
		oracledb.getConnection({
				user: dbConfig.user,
				password: dbConfig.password,
				connectString: dbConfig.connectString
			},
			function(err, conn) {
				if (err) {
					console.error(err.message);
					return;
				}
				connection = conn;
				return resolve(conn)
			})
	});
}

function generateInsertTest(size,string) {
	var inString = randomString(size);
	var testId = getNewId();
	console.log("IN STR length " + inString.length)
	console.log("IN STR binarySize " + formatBytes(strLength(inString)));
	if(string){
		inString = string;
	}

	return doQuery('INSERT INTO "test" ("ID","DATA") VALUES (:id,:data)', {
			data: inString,
			id: testId
		}).then(data => {
			expect(data).toBeDefined();
		})
		.then(doQuery('SELECT * FROM "test" WHERE "ID"=(:id)', {
			id: testId
		}).then(function(results) {
			return streamToString(results.rows[0][1]).then(function(outString) {
				console.log("OUT STR length " + outString.length)
				console.log("OUT STR binarySize " + formatBytes(strLength(outString)));
				expect(inString.length).toBe(outString.length)
			}).catch(function(err) {
				console.log(err)
			})
		}))

	.catch(err => {
		console.log(err)
		expect(err).toBeNull();
	});
}

function generateBufferInsertTest(size) {
	var inString = randomString(size);
	var testId = getNewId();
	console.log("IN STR length " + inString.length)
	console.log("IN STR binarySize " + formatBytes(strLength(inString)));
	return doBufferedInsertQuery('INSERT INTO "test" ("ID","DATA") VALUES (' + testId + ',EMPTY_CLOB()) RETURNING "DATA" INTO :lobbv', {
		data: inString
	})
}

function generateBufferInsertFromFileTest(filepath) {
	var testId = getNewId();
	return doBufferedInsertQuery('INSERT INTO "test" ("ID","DATA") VALUES (' + testId + ',EMPTY_CLOB()) RETURNING "DATA" INTO :lobbv', {
		file: filepath
	})
}

beforeAll(() => {
	return connect();
});

afterAll(() => {
	return doRelease();
});

test('Basic query', () => {
	expect.assertions(1);
	return doQuery("SELECT SYS_CONTEXT ('USERENV', 'HOST') FROM DUAL", []).then(data => {
		console.log(JSON.stringify(data))
		expect(data).toBeDefined();
	});
})

test('Create table test', () => {
	return doQuery('DROP TABLE "test"', [], true)
		.then(function() {
			console.log("Table dropped")
		})
		.then(doQuery('CREATE TABLE "test" ( ID number(10), data CLOB )', []))
		.then(data => {
			console.log("Table created")
		})
		.catch(function(err) {
			console.log(err)
		});
})

// test('10mb buffered insert to clob', () => {
// 	return generateBufferInsertTest(10000000)
// })

// test('100mb buffered from file to clob', () => {
// 	return generateBufferInsertFromFileTest(__dirname + "/100mb.txt")
// })

// test('100000 string length insert into CLOB', () => {
// 	generateInsertTest(100000)
// })

// test('200000 string length insert into CLOB', () => {
// 	generateInsertTest(200000)
// })

// test('1000000 string length insert into CLOB', () => {
// 	generateInsertTest(1000000)
// })

// test('100mb string length insert into CLOB', () => {
// 	generateInsertTest(10000000)
// })

// test('100mb buffered insert to clob', () => {
// 	return generateBufferInsertTest(100000000)
// })

// test('Insert basic XML into CLOB', () => {
// 	var xml = fs.readFileSync(__dirname+'/example.xml','utf8')
// 	generateInsertTest(0,xml)
// })

test('Insert basic JSON converted to XML into CLOB', () => {
	var json = JSON.parse(fs.readFileSync(__dirname+'/example.json','utf8'))
	var builder = new xml2js.Builder();
	var xml = builder.buildObject(json);
	generateInsertTest(0,xml)
})

test('Query from XML', () => {
	return doQuery('SELECT XMLTYPE("test"."DATA").EXTRACT(\'//Company/Employee/FirstName/text()\').getStringVal() FROM "test"', []).then(data => {
		console.log(JSON.stringify(data))
	});
})


/*
SELECT XMLTYPE("test"."DATA").EXTRACT('//Company/Employee/FirstName/text()').getStringVal() FROM "test";
 */
//SELECT  NVL(length("test"."DATA"),0)  FROM "test";

