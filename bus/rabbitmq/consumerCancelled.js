// [AI] Reports a consumer cancelled by the broker (queue deleted, or the node hosting a classic queue went down) as an
// error on the bus: the connection and the channel stay open, so nothing else reports it and the queue would be left
// without a consumer. One node going down cancels every queue it hosts at once: the error is emitted once per bus, so
// its owner reconnects once instead of once per queue.
module.exports = function consumerCancelled (bus, queueName) {
  if (bus.consumerCancelled) {
    return;
  }
  bus.consumerCancelled = true;
  bus.emit('error', new Error('Consumer cancelled by the broker on queue ' + queueName));
};
