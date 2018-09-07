from __future__ import absolute_import

from statsd import StatsClient

import logging


_log = logging.getLogger(__name__)


class StatsdAdapter(object):
    """
    This adapter allows us to pass tags to stats loggers that support tags (e.g. DogStatsD), while
    instead swallowing those tags for the loggers that _don't_ support tags (e.g. statsd.StatsClient)
    """
    def __init__(self, inner):
        self._inner = inner

    def timer(self, stat, rate=1, tags=None):
        self._inner.timed(stat=stat, rate=rate)

    def timing(self, stat, delta, rate=1, tags=None):
        """Send new timing information. `delta` is in milliseconds."""
        self._inner.timing(stat=stat, delta=delta, rate=rate)

    def incr(self, stat, count=1, rate=1, tags=None):
        """Increment a stat by `count`"""
        self._inner.increment(metric=stat, value=count, sample_rate=rate)

    def decr(self, stat, count=1, rate=1, tags=None):
        """Decrement a stat by `count`"""
        self._inner.decrement(metric=stat, value=count, sample_rate=rate)

    def gauge(self, stat, value, rate=1, delta=False, tags=None):
        """Set a gauge value"""
        self._inner.gauge(metric=stat, value=value, sample_rate=rate)

    def set(self, stat, value, rate=1, tags=None):
        """Set a set value"""
        self._inner.set(metric=stat, value=value, sample_rate=rate)

    def histogram(self, stat, value, rate=1, tags=None):
        """This is not supported by statsd.StatsClient. Use with caution!"""
        _log.warning("Histograms are not supported by statsd")
