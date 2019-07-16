FlashFlood is an event recorder and streamer built on top of [AWS S3](https://aws.amazon.com/s3/),
supporting distributed writes and fast distributed bulk reads.

## Installation
    pip install flashflood

## Usage
Instantiate into your favorite bucket and prefix
```
from flashflood import FlashFlood
ff = FlashFlood("my_bucket", "my_prefix")
```

Record events (this method may be called in parallelized or distributed environments)
```
ff.put(event_data, event_uuid, event_date)
```

To optimize bulk reads, call `collate` (this must be executed non-concurrently)
```
ff.collate(number_of_events=5000)
```

Get an event
```
ff.get_event(my_event_id)
```

Update event data
```
ff.update_event(my_new_event_data, my_event_id)
```

Stream events
```
for event in ff.events(from_date=date_a, to_date=date_b):
    my_event_processor(event.data)
```

Stream events from S3 signed urls:
```
url_info = ff.event_urls(from_date=date, to_date=date_b)
for event in flashflood.events_from_urls(url_info, from_date=date_a, to_date=date_b):
    my_event_processor(event.data)
```
