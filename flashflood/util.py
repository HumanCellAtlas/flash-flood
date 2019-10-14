import time
import typing
import datetime
from string import hexdigits
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import AbstractContextManager

import boto3

def datetime_to_timestamp(dt):
    return dt.strftime("%Y-%m-%dT%H%M%S.%fZ")

def datetime_from_timestamp(ts):
    return datetime.datetime.strptime(ts, "%Y-%m-%dT%H%M%S.%fZ")

def timestamp_now():
    return datetime_to_timestamp(datetime.datetime.utcnow())

class DateRange:
    def __init__(self, start: datetime.datetime=None, end: datetime.datetime=None):
        self.start = start or datetime.datetime.min
        self.end = end or datetime.datetime.max
        assert self.start <= self.end

    def overlaps(self, other):
        """
        Test if [self.start : self.end] overlaps with (other.start : other.end]
        """
        a = self
        b = other
        if b.start <= a.start and a.end <= b.end:
            return True
        elif a.start <= b.start and b.end <= a.end:
            return True
        elif a.start <= b.start and b.start < a.end:
            return True
        elif a.start <= b.end and b.end <= a.end:
            return True
        return False

    def contains(self, date: datetime.datetime):
        """
        Test if (self.start : self.end] contains date
        """
        return self.start < date and date <= self.end

    def __contains__(self, item: typing.Any):
        if isinstance(item, datetime.datetime):
            return self.contains(item)
        elif isinstance(item, type(self)):
            return item.overlaps(self)
        else:
            raise TypeError(f"expected datetime instance or {type(self)} instance")

    @property
    def past(self):
        if datetime.datetime.min == self.start:
            return _EmptyDateRange()
        else:
            return type(self)(datetime.datetime.max, self.start)

    @property
    def future(self):
        if datetime.datetime.max == self.end:
            return _EmptyDateRange()
        else:
            return type(self)(self.end, datetime.datetime.max)

class _EmptyDateRange(DateRange):
    """
    This class represents an empty date range
    """
    def overlaps(self, *args, **kwargs):
        return False

    def contains(self, *args, **kwargs):
        return False

_S3_BATCH_DELETE_MAX_KEYS = 1000

def delete_keys(bucket, keys, number_of_workers=4):
    def _delete(keys_to_delete):
        assert _S3_BATCH_DELETE_MAX_KEYS >= len(keys_to_delete)
        objects = [dict(Key=key) for key in keys_to_delete]
        bucket.delete_objects(Delete=dict(Objects=objects))

    if keys:
        with ThreadPoolExecutor(max_workers=number_of_workers) as e:
            futures = list()
            while keys:
                futures.append(e.submit(_delete, keys[:_S3_BATCH_DELETE_MAX_KEYS]))
                keys = keys[_S3_BATCH_DELETE_MAX_KEYS:]
            for f in as_completed(futures):
                f.result()

class S3Deleter(AbstractContextManager):
    def __init__(self, bucket, deletion_threshold=5 * _S3_BATCH_DELETE_MAX_KEYS, number_of_workers=4):
        self.bucket = bucket
        self.deletion_threshold = deletion_threshold
        self._keys = list()
        self.number_of_workers = number_of_workers

    def delete(self, key):
        self._keys.append(key)

    def __exit__(self, *args, **kwargs):
        delete_keys(self.bucket, self._keys)

def concurrent_listing(bucket, prefixes, number_of_workers=4):
    """
    Concurrently list objects from `bucket` for `prefixes`. Lexicographical ordering is lost.
    """
    assert not isinstance(prefixes, str)

    def _list(pfx):
        # TDOD: handle or limit unbounded listing into memory
        return [item for item in bucket.objects.filter(Prefix=pfx)]

    with ThreadPoolExecutor(max_workers=number_of_workers) as e:
        futures = [e.submit(_list, f"{pfx}") for pfx in prefixes]
        for f in as_completed(futures):
            for item in f.result():
                yield item
