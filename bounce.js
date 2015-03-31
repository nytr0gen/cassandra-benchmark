/**
 * Created by robert on 27/02/15.
 */
var cassandra = require('cassandra-driver');
var client = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    keyspace: 'test'
});
//client.on('log', function(level, className, message, furtherInfo) {
//    console.log('log event: %s -- %s', level, message);
//});
var uuid = require('node-uuid').v1;

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

var shuffle = function(array) {
    var n = array.length, i, tmp;
    while (n) {
        i = Math.floor(Math.random() * n--);

        tmp = array[i];
        array[i] = array[n];
        array[n] = tmp;
    }

    return array;
};

console.time('test');
var data = [];
var dataInterval = setInterval(function() {
    console.log(data.length);
}, 2000);
var testNum = Math.random() * 200 | 0;
client.eachRow('SELECT id, md5 FROM test.users LIMIT 10000000', [], {autoPage: true},
    function(n, row) {
        if (n >= testNum) {
            data.push({
                'id': row.id.toString(),
                'md5': row.md5
            });
            testNum += Math.random() * 200 | 0;
            if (testNum >= 5000) {
                testNum = Math.random() * 200 | 0;
            }
        }
    },
    function (err) {
        console.log(err);
        clearInterval(dataInterval);
        console.log(data.length);

        data = shuffle(data);
        for (var i = 0; i < data.length; i++) {
            insert(data[i].id, data[i].md5);
        }
    }
);



var info={
    inserted: 0,
    errored: 0,
    toInsert: 10000
};
var f = function() {
    console.log(info);
};

var insert = function(id, md5) {
    client.execute('INSERT INTO test.bounce (id, md5) VALUES(?, ?);', [id, md5], function(err) {
        if (err) {
            console.log(err);
            info.errored++;
        } else {
            info.inserted++;
        }
        //if (info.inserted + info.errored >= info.toInsert) {
        //console.timeEnd('test');
        //console.log('finished');
        //process.exit(0);
        //}
        if (info.inserted % 1000 == 0) f();
    });
};
