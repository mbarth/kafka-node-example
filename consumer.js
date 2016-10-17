var path = require('path'),
    sentiment = require('sentiment'),
    debug = require('debug')('kafka-node:sample-consumer'),
    kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.Client(),
    currentTopic = 'twitter',
    consumer = new Consumer(
        client,
        [
            { topic: currentTopic, partition: 0 }
        ],
        {
            autoCommit: false
        }
    );

var tweetTotalSentiment = 0;
var tweetCount = 0;

/**
 * Test method. Experimenting with parent kicking off script and changing topics.
 */
process.on('message', function(message) {
    debug(path.basename(__filename), 'script received message:', message);
    var object = JSON.parse(message);
    if (object.topicName) {
        consumer.removeTopics([currentTopic], function(err, removed){
            if (err) {
                console.error(err);
                process.exit(1);
            }
            debug('removed current topic', currentTopic);
            debug(removed);
        });
        currentTopic = object.topicName;
        consumer.addTopics([object.topicName], function(err, added){
            if (err) {
                console.error(err);
                process.exit(1);
            }
            debug('added new topic', currentTopic);
            debug(added);
        });
    }
});

consumer.on('message', function (message) {
    var tweet = JSON.parse(message.value);
    sentiment(tweet.text, function (err, result) {
        debug(getSentimentDescription());
        tweetCount++;
        tweetTotalSentiment += result.score;
    });
});

consumer.on('error', function(err) {
    console.error('here' ,err);
});

function getSentimentDescription() {
    var avg = tweetTotalSentiment / tweetCount;
    return rateScore(avg);
}

function rateScore(score) {
    if (score > 0.5) { // happy
        return "HAPPY";
    }
    if (score < -0.5) { // angry
        return "ANGRY";
    }
    // neutral
    return "NEUTRAL";
}