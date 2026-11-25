// [AI] Catch handler for the promises of the channel setup (exchange and queue declarations, bindings, consume, channel
// creation). When the broker refuses one of them (e.g. 404 NOT_FOUND while the node hosting a classic queue is down,
// 406 PRECONDITION_FAILED), it closes the channel: the operations still pending on it are rejected too ("Channel ended,
// no reply will be forthcoming"). Left unhandled, those rejections crash the process. The failure itself is already
// reported by the channel 'error' event (or the connection 'close' event), which the bus emits and its owner reconnects
// on, so the rejection is only logged here.
module.exports = function logSetupFailure (log, operation) {
  return function (err) {
    log('%s failed: %s', operation, err && err.message);
  };
};
