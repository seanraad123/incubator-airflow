from __future__ import absolute_import
from datadog.dogstatsd import DogStatsd

import logging


_log = logging.getLogger(__name__)


class DogStatsdAdapter(object):
    """
    A wrapper around DogStatsd that supports the full statsd.StatsClient interface
    Note that `tags` is available on all these methods, but this is not supported by statsd.StatsClient. It has been
    added, but should only be used when you are CERTAIN that you will be using this class (or something similar)
    """

    def __init__(self, host='localhost', port=8125, prefix=None, maxudpsize=512, ipv6=False):
        if ipv6:
            _log.warning("DogStatsdAdapter() was 'ipv6'. This is ignored")

        self._dd_client = DogStatsd(
            host=host,
            port=port,
            namespace=prefix,
            max_buffer_size=maxudpsize
        )

    def timer(self, stat, rate=1, tags=None):
        self._dd_client.timed(metric=stat, sample_rate=rate, use_ms=True, tags=tags)

    def timing(self, stat, delta, rate=1, tags=None):
        """Send new timing information. `delta` is in milliseconds."""
        self._dd_client.timing(metric=stat, value=delta, sample_rate=rate, tags=tags)

    def incr(self, stat, count=1, rate=1, tags=None):
        """Increment a stat by `count`."""
        self._dd_client.increment(metric=stat, value=count, sample_rate=rate, tags=tags)

    def decr(self, stat, count=1, rate=1, tags=None):
        """Decrement a stat by `count`."""
        self._dd_client.decrement(metric=stat, value=count, sample_rate=rate, tags=tags)

    def gauge(self, stat, value, rate=1, delta=False, tags=None):
        """Set a gauge value."""
        self._dd_client.gauge(metric=stat, value=value, sample_rate=rate, tags=tags)
        if delta:
            _log.warning("DogStatsdAdapter was passed a gauge with 'delta' set. This is ignored in datadog")

    def set(self, stat, value, rate=1, tags=None):
        """Set a set value."""
        self._dd_client.set(metric=stat, value=value, sample_rate=rate, tags=tags)

    def histogram(self, stat, value, rate=1, tags=None):
        """
        Sample a histogram value, optionally setting tags and a sample rate.
        This is not supported by statsd.StatsClient. Use with caution!
        """
        self._dd_client.set(metric=stat, value=value, sample_rate=rate, tags=tags)
