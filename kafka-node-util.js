var kafka = require('kafka-node'),
    debug = require('debug')('kafka-node-util'),
    utils = require('./utils');

function KafkaNodeUtil(config) {
    if (!(this instanceof KafkaNodeUtil)) return new KafkaNodeUtil(config);

    var defaults = {
        connectionString: 'localhost:2181/',
        autoCommit: false,
        fetchMaxWaitMs: 1000,
        fetchMaxBytes: 1024 * 1024,
        fromOffset: false,
        partition: 0
    };
    this.config = utils.merge(defaults, config);
}

module.exports = KafkaNodeUtil;

KafkaNodeUtil.prototype.listenOnTopic = function(topic, callback) {
    var client = new kafka.Client(this.config.connectionString);
    var consumer = null;
    var options = {
        autoCommit: this.config.autoCommit,
        fetchMaxWaitMs: this.config.fetchMaxWaitMs,
        fetchMaxBytes: this.config.fetchMaxBytes,
        fromOffset: false
    }
    if (this.config.fromOffset) {
        var offset = new kafka.Offset(client);
        var partition = this.config.partition;
        options.fromOffset = true;
        offset.fetch([{topic: topic, time: -1}], function (err, results) {
            if (err) console.error(err);
            var latestOffset = results[topic][partition][0];
            var topics = [{topic: topic, partition: partition, offset: latestOffset}];
            debug('Kafka Consumer, listening on:', topics);
            consumer = new kafka.Consumer(client, topics, options);
            listenOnConsumers();
        });
    } else {
        var Consumer = kafka.Consumer;
        var topics = [{topic: topic, partition: this.config.partition}];
        debug('Kafka Consumer, listening on:', topics);
        consumer = new Consumer(client,topics,options);
        listenOnConsumers();
    }

    function listenOnConsumers() {
        consumer.on('error', function (err) {
            console.error(err);
        });
        consumer.on('message', function (message) {
            if ( typeof callback === 'function' ) callback(message.value);
        });
    }
}