/**
 * Created by robert on 27/02/15.
 */
var cassandra = require('cassandra-driver');
var client = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    keyspace: 'test',
    authProvider: new cassandra.auth.PlainTextAuthProvider('cassandra', 'cassandra')
});
//client.on('log', function(level, className, message, furtherInfo) {
//    console.log('log event: %s -- %s', level, message);
//});
var uuid = require('node-uuid').v1;
var info={
    inserted: 0,
    errored: 0,
    toInsert: 10000
};

var randHex = function(num, base) {
    var base = base || 16;
    var ret = '';
    while (num--) {
        ret += Math.floor((1 + Math.random()) * 0x100).toString(base).substring(1);
    }

    return ret;
};

var email = function() {
    return randHex(10, 36) + '@yahoo.com';
};

var md5 = function() {
    return randHex(16);
};

var f = function() {
    console.log(info);
}; f();

console.time('test');
for (var i = 0; i < info.toInsert; i++)
    client.execute('INSERT INTO test.users (id, email, md5) VALUES(?, ?, ?);', [uuid(), email(), md5()], function(err) {
        if (err) {
            console.log(err);
            info.errored++;
        } else {
            info.inserted++;
        }
        if (info.inserted + info.errored >= info.toInsert) {
            console.timeEnd('test');
            console.log('finished');
            process.exit(0);
        }
        if (info.inserted % 1000 == 0) f();
    });