var path = require('path')
    , util = require('util')
    , WebSocketServer = require('websocket').server
    , http = require('http')
    , debug = require('debug')('kafka-consumer')
    , currentFilter = ''
    , argv = require('minimist')(process.argv.slice(2))
    , fromOffset = process.env.FROM_OFF_SET
    , report_period = process.env.REPORT_INTERVAL
    , kafkaTopic = argv['topic'] || process.env.KAFKA_TOPIC_NAME
    , port = argv['port']
    , help = (argv.help || argv.h);

if (help || port === undefined || kafkaTopic === undefined) {
    console.log("Kafka Consumer / Tweet analyzer");
    console.log();
    console.log("Usage: node " + path.basename(__filename));
    console.log("                             --port <port> [--topic <topic>]");
    console.log();
    process.exit(help ? 0 : 1);
}

/**
 * Set up web socket listener
 */
var server = http.createServer(function (request, response) {
    debug(new Date() + ' Received request for ' + request.url);
    response.writeHead(404);
    response.end();
});
server.listen(port, function () {
    debug((new Date()) + ' Server is listening on port ' + port);
});
var wsServer = new WebSocketServer({
    httpServer: server,
    autoAcceptConnections: true, // default to true
    maxReceivedFrameSize: 64 * 1024 * 1024,   // 64MiB
    maxReceivedMessageSize: 64 * 1024 * 1024, // 64MiB
    fragmentOutgoingMessages: false,
    keepalive: false,
    disableNagleAlgorithm: false
});
wsServer.on('connect', function (conn) {
    debug(new Date() + ' Connection accepted - Protocol Version ' + conn.webSocketVersion);
    conn.on('close', function (reasonCode, description) {
        debug(new Date() + ' ' + reasonCode + ': [' + description +
            '] Peer [' + conn.remoteAddress +
            '] disconnected.');
        conn._debug.printOutput();
    });
});

/**
 * Set up a kafka consumer
 */
var KafkaNodeUtil = require('./kafka-node-util');
var config = { fromOffset: fromOffset };
// using util class that handles retrieving messages from latest offset
var knu = new KafkaNodeUtil(config);
knu.listenOnTopic(kafkaTopic, processTweet)

/**
 * Twitter Message analysis logic
 */
var usernames = {};

function processTweet(rawMessageValue) {
    var tweet = JSON.parse(rawMessageValue);
    if (tweet != undefined && tweet.handle) {
        if (tweet.filter != currentFilter) {
            usernames = {};
            currentFilter = tweet.filter;
        }
        var username = tweet.handle;
        if (usernames[username] === undefined)
            usernames[username] = {
                'username': username,
                'tweets': 0
            };
        usernames[username].tweets += 1;
    }
}

// Broadcast to web socket clients
var reportInterval = setInterval(function () {
    var number_of_connections = wsServer.connections.length;
    if (number_of_connections > 0) {
        var topUsersLimit = 10;
        var sorted_users = [];
        var topUsers = [];
        for (var userKey in usernames) {
            var user = usernames[userKey];
            sorted_users.push(user);
        }
        sorted_users.sort(function (a, b) {
            return (a.tweets > b.tweets ? -1 : (a.tweets == b.tweets ? 0 : 1));
        });
        for (var i = 0; i < Math.min(topUsersLimit, sorted_users.length); i++) {
            topUsers.push(sorted_users[i])
        }

        var userMessage = {user_stats: topUsers};
        var jsonMessage = JSON.stringify(userMessage);
        debug(new Date() + ' sending =>\n' + jsonMessage);
        wsServer.broadcastUTF(jsonMessage, function(err) {
            if (err) {
                console.error('sendUTF() error: ' + err);
                clearInterval(reportInterval);
            }
        });
    }
}, report_period);