var debug = require('debug')('kafka-node:twitter-stream-producer'),
    path = require('path'),
    argv = require('minimist')(process.argv.slice(2)),
    util = require('util'),
    Twitter = require('node-tweet-stream'),
    envs = require('envs'),
    kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client(), // using default values of 'localhost:2181'
    producer = new Producer(client);

var environment = envs('NODE_ENV', 'production');
// Twitter arguments
var consumer_key = argv['consumer-key'] || process.env.TWITTER_CONSUMER_KEY;
var consumer_secret = argv['consumer-secret'] || process.env.TWITTER_CONSUMER_SECRET;
var access_key = argv['access-key'] || process.env.TWITTER_ACCESS_KEY;
var access_secret = argv['access-secret'] || process.env.TWITTER_ACCESS_SECRET;
// Kafka arguments
var topicName = argv.topic;
// General arguments
// Number of tweets to buffer before calling send()
var message_buffer_size = argv['message-buffer-size'] || 100;
var filter = argv.filter;
var help = (argv.help || argv.h);

if (help ||
    consumer_key === undefined || consumer_secret === undefined ||
    access_key === undefined || access_secret === undefined ||
    topicName === undefined)
{
    console.log("Stream tweets and load them into a Kafka topic.");
    console.log();
    console.log("Usage: node " + path.basename(__filename) + " [--consumer-key <consumer-key>] [--consumer-secret <consumer-secret>]");
    console.log("                             [--access-key <access-key>] [--access-secret <access-secret>]");
    console.log("                             --topic <topic> [--filter <term>] [--message-buffer-size <num-messages>]");
    console.log();
    console.log("You can also specify Twitter credentials via environment variables: TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, TWITTER_ACCESS_KEY, TWITTER_ACCESS_SECRET.");
    process.exit(help ? 0 : 1);
}

var twitter_stream = new Twitter({
    consumer_key: consumer_key,
    consumer_secret: consumer_secret,
    token: access_key,
    token_secret: access_secret
});

var kafka_producer_ready_flag = false;
var started = Date.now();
// make Stream globally visible so clean up is better
var global_stream = null;
var consumed_messages_list = [];
var messages_sent_count = 0;
var messages_acked_count = 0;
var exiting = false;
// Windowed stats
var windowed_started = Date.now();
var windowed_collected = 0;
var windowed_acked_count = 0;
var windowed_period = 10000; // report every 10 secs
var windowed_report_interval = setInterval(reportWindowedRate, windowed_period);

/**
 * Start
 */
debug('Starting in >', environment, '< environment');
producer.on('ready', function () {
    debug('Kafka Producer is NOW ready');
    listenForTwitterStreamEvents();
});

function listenForTwitterStreamEvents() {
    if (filter) {
        twitter_stream.track(filter);
    }
    twitter_stream.language('en');

    twitter_stream.on('tweet', function (data) {
        if (exiting) return;
        // Filter to only tweets. Streaming data may contain other data and events such as friends lists, block/favorite
        // events, list modifications, etc. Here we prefilter the data, although this could also be done downstream on
        // the consumer if we wanted to preserve the entire global_stream. The heuristic we use to distinguish real tweets (or
        // retweets) is the text field, which shouldn't appear at the top level of other events.
        if (data.text === undefined)
            return;
        var twitter_data = {
            'handle': data.user.screen_name,
            'text': data.text,
            'device': data.source,
            'filter': filter ? filter : ''
        };
        // Serialize twitter_data to a string
        var message = JSON.stringify(twitter_data);
        consumed_messages_list.push(message);
        windowed_collected++;
        // Send if we've hit our buffering limit. The number of buffered messages balances your tolerance for losing data
        // (if the process/host is killed/dies) against throughput (batching messages into fewer requests makes processing
        // more efficient).
        if (consumed_messages_list.length >= message_buffer_size) {
            messages_sent_count += consumed_messages_list.length;
            var responseHandler = handleProduceResponse.bind(undefined, consumed_messages_list.length);
            var payload = [
                {topic: topicName, messages: consumed_messages_list}
            ];
            producer.send(payload, responseHandler);
            // reset messages list
            consumed_messages_list = [];
        }
    });
}

function handleProduceResponse(batch_messages_count, err, res) {
    messages_acked_count += batch_messages_count;
    windowed_acked_count += batch_messages_count;
    if (err) {
        if (err == 'LeaderNotAvailable' ||
            err.message == 'UnknownTopicOrPartition') {
            resetTopic(topicName);
        } else {
            debug("Error sending data to specified topic: " + err);
            shutdown();
        }
    }
    checkExit();
}

function resetTopic(topicName) {
    debug('Topic [' + topicName + '] does not or no longer exists ... attempting to create it');
    producer.createTopics(topicName, false, function (err, data) {
        if (err) {
            debug('Failed to create topic [' + topicName + ']', err);
            shutdown();
        }
        debug('Created new topic [' + topicName + ']', data);
    });
    var now = Date.now();
    messages_acked_count = 0;
    messages_sent_count = 0;
    windowed_started = now;
    windowed_collected = 0;
    windowed_acked_count = 0;
}

/**
 * Report how many messages where collected and sent to Kafka
 */
function reportWindowedRate() {
    if (kafka_producer_ready_flag) {
        var now = Date.now();
        debug("Collected " + windowed_collected + " tweets and stored " +
            windowed_acked_count + " to Kafka in " + Math.round((now-windowed_started)/1000) +
            "s, " + Math.round(windowed_collected / ((now - windowed_started) / 1000)) + " tweets/s");
        windowed_started = now;
        windowed_collected = 0;
        windowed_acked_count = 0;
    }
}

function checkExit() {
    if (exiting && messages_acked_count == messages_sent_count) {
        var finished = Date.now();
        debug("Converted " + messages_acked_count + " tweets, dropped last " +
            consumed_messages_list.length + " remaining buffered tweets, " +
            Math.round(messages_acked_count/((finished-started)/1000)) + " tweets/s");
        exiting = false;
    }
}

function shutdown() {
    debug("**** Gracefully shutting down from SIGINT (Ctrl-C) ****");
    exiting = true;
    if (global_stream) global_stream.destroy();
    clearInterval(windowed_report_interval);
    checkExit();
    process.exit(0);
}

// Gracefully shutdown on Ctrl-C
process.on('SIGINT', shutdown);