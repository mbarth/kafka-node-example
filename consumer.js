var path = require('path'),
    debug = require('debug')('kafka-node:sample-consumer'),
    kafkaTopic = 'twitter';

/**
 * Set up a kafka consumer
 */
var KafkaNodeUtil = require('./kafka-node-util');
var config = {fromOffset: true};
// using util class that handles retrieving messages from latest offset
var knu = new KafkaNodeUtil(config);
knu.listenOnTopic(kafkaTopic, processTweet)

function processTweet(rawMessageValue) {
    var tweet = JSON.parse(rawMessageValue);
    debug(tweet);
}