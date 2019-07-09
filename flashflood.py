import io
import datetime
import json
import requests
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

s3 = boto3.resource("s3")
s3_client = boto3.client("s3")
  
def timestamp_now():
    timestamp = datetime.datetime.utcnow()
    return timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

def datetime_from_timestamp(ts):
    return datetime.datetime.strptime(ts, "%Y-%m-%dT%H%M%S.%fZ")

distant_past = datetime_from_timestamp("0001-01-01T000000.000000Z")
far_future = datetime_from_timestamp("5000-01-01T000000.000000Z")
ID_PART_DELIMITER = "--"
RINDEX_DELIMITER = ID_PART_DELIMITER + "-"

class FlashFloodCollationError(Exception):
    pass

class FlashFlood:
    def __init__(self, bucket, root_prefix):
        self.bucket = s3.Bucket(bucket)
        self.root_prefix = root_prefix
        self._collation_pfx = f"{root_prefix}/collations"
        self._new_pfx = f"{root_prefix}/new"
        self._rindex_pfx = f"{root_prefix}/rindex"

    def put(self, data, event_id: str=None, timestamp: str=None):
        timestamp = timestamp or timestamp_now()
        event_id = event_id or str(uuid4())
        assert ID_PART_DELIMITER not in timestamp
        assert ID_PART_DELIMITER not in event_id
        name = timestamp + ID_PART_DELIMITER + event_id
        self.bucket.Object(f"{self._new_pfx}/{name}").upload_fileobj(io.BytesIO(data))
        return event_id

    def get(self, event_id):
        """ Look in reverse index """
        search_key = f"{self._rindex_pfx}/{event_id}"
        for item in self.bucket.objects.filter(Prefix=search_key):
            rindex_id = item.key.split("/", 1)[1]
            collation_id = rindex_id.split(RINDEX_DELIMITER, 1)[1]
            collation_key = f"{self._collation_pfx}/{collation_id}"
            index = json.loads(self.bucket.Object(collation_key + ".index").get()['Body'].read().decode("utf-8"))
            for record in index:
                if record['event_id'] == event_id:
                    start_byte = record['start']
                    end_byte = start_byte + record['size'] - 1
                    event = self.bucket.Object(collation_key).get(Range=f"bytes={start_byte}-{end_byte}")['Body'].read()
                    return event

    def collate(self, minimum_number_of_events=10):
        index = list()
        data = b""
        for timestamp, event_id, part_data in self._get_collation_parts():
            index.append(dict(event_id=event_id, timestamp=timestamp, start=len(data), size=len(part_data)))
            data += part_data
        if len(index) < minimum_number_of_events:
            raise FlashFloodCollationError("Not enough events to collate")
        collation_id = index[0]['timestamp'] + ID_PART_DELIMITER + index[-1]['timestamp']
        collation_key = f"{self._collation_pfx}/{collation_id}"
        collation_blob = self.bucket.Object(collation_key)
        with io.BytesIO(data) as fh:
            collation_blob.upload_fileobj(fh)
        with io.BytesIO(json.dumps(index).encode("utf-8")) as fh:
            self.bucket.Object(f"{collation_key}.index").upload_fileobj(fh)
        self._write_rindex(collation_id, [i['event_id'] for i in index])
        self._delete_new_events(index)
        return collation_id

    def _get_collation_parts(self, max_number_of_parts=1000):
        def _get_blob(blob):
            key = blob.key
            fqid = key.rsplit("/", 1)[1]
            timestamp, event_id = fqid.rsplit(ID_PART_DELIMITER, 1)
            return timestamp, event_id , blob.get()['Body'].read()
            
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = list()
            for i, blob in zip(range(max_number_of_parts), self.bucket.objects.filter(Prefix=self._new_pfx)):
                futures.append(e.submit(_get_blob, blob))
            parts = [f.result() for f in as_completed(futures)]
        parts.sort(key=lambda x: x[0])
        return parts

    def get_collation_index(self, collation_id):
        collation_key = f"{self._collation_pfx}/{collation_id}.index"
        data = self.bucket.Object(collation_key).get()['Body'].read()
        return json.loads(data.decode("utf-8"))

    def _collation_info(self, key):
        collation_id = key.rsplit("/", 1)[1]
        start_date, end_date = collation_id.split(ID_PART_DELIMITER)
        return collation_id, datetime_from_timestamp(start_date), datetime_from_timestamp(end_date)

    def events(self, from_date=distant_past, to_date=far_future):
        for collation_blob in self.bucket.objects.filter(Prefix=self._collation_pfx):
            if not collation_blob.key.endswith(".index"):
                collation_id, start_date, end_date = self._collation_info(collation_blob.key)
                if start_date >= from_date and end_date <= to_date:
                    index = self.get_collation_index(collation_id)
                    data = collation_blob.get()['Body']
                    for i in index:
                        part_data = data.read(i['size'])
                        yield i['timestamp'], i['event_id'], part_data

        for event_blob in self.bucket.objects.filter(Prefix=self._new_pfx):
            key = event_blob.key
            fqid = key.rsplit("/", 1)[1]
            timestamp, event_id = fqid.rsplit(ID_PART_DELIMITER, 1)
            yield timestamp, event_id, event_blob.get()['Body'].read()

    def get_presigned_event_urls(self, from_date=distant_past, to_date=far_future):
        urls = list()
        for collation_blob in self.bucket.objects.filter(Prefix=self._collation_pfx):
            if not collation_blob.key.endswith(".index"):
                collation_id, start_date, end_date = self._collation_info(collation_blob.key)
                if start_date >= from_date and end_date <= to_date:
                    collation_key = f"{self._collation_pfx}/{collation_id}"
                    params = dict(Bucket=self.bucket.name, Key=collation_key + ".index")
                    index_url = s3_client.generate_presigned_url(ClientMethod="get_object",
                                                                 Params=params)
                    params['Key'] = collation_key
                    collation_url = s3_client.generate_presigned_url(ClientMethod="get_object",
                                                                     Params=params)
                    urls.append(dict(index=index_url, events=collation_url))
        return urls

    def _write_rindex(self, collation_id, event_ids):
        def _write_rindex(event_id, collation_id):
            key = f"{self._rindex_pfx}/{event_id}" + RINDEX_DELIMITER + collation_id
            with io.BytesIO(b"") as fh:
                self.bucket.Object(key).upload_fileobj(fh)

        with ThreadPoolExecutor(max_workers=10) as e:
            for f in as_completed([e.submit(_write_rindex, event_id, collation_id) for event_id in event_ids]):
                f.result()

    def _delete_new_events(self, collation_index):
        def _delete(i):
            key = f"{self._new_pfx}/{i['timestamp']}.{i['event_id']}"
            self.bucket.Object(key).delete()

        with ThreadPoolExecutor(max_workers=10) as e:
            for f in as_completed([e.submit(_delete, i) for i in collation_index]):
                f.result()

    def _delete_all(self):
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(blob.delete)
                       for blob in self.bucket.objects.filter(Prefix=self.root_prefix)]
            for f in as_completed(futures):
                f.result()

def events_for_presigned_urls(url_info):
    for urls in url_info:
        index_url = urls['index']
        events_url = urls['events']
        resp = requests.get(index_url)
        resp.raise_for_status()
        index = resp.json()
        resp = requests.get(events_url, stream=True)
        for item in index:
            yield item['timestamp'], item['event_id'], resp.raw.read(item['size'])
