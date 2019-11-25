# Changes for v0.4.3 (2019-11-25)
Handle new events with simultaneous timestamps (#46)
Confirm s3 writes during testing (#44)
Update README.md

# Changes for v0.4.2 (2019-11-07)
Use new upload utility for object uploads (#42)
Add s3 object tagging update utility (#41)
Add s3 upload utility (#40)
Make base s3 client available to flashflood objects (#39)
Fix flaky event replay after update test (#38)

# Changes for v0.4.1 (2019-10-18)
List journals starting from know journal id (#37)

# Changes for v0.4.0 (2019-10-16)
Refactored flash-flood interface (#32)
Add tests for objects and identifiers (#31)
Add exceptions module (#30)
Add interface classes to object IDs (#29)
Add interface classes to journals and updates (#28)
Add S3 key index (#26)
Small updates to utility methods (#27)
Add listing utility method, extend utility tests (#24)
Add status output to event journaling and update (#23)

# Changes for v0.3.0 (2019-10-01)
Eventually consistent update/delete (#22)
Ensure root prefix doesn't end with "/" (#21)

# Changes for v0.2.1 (2019-09-28)
Add event_exists method (#20)

# Changes for v0.2.0 (2019-09-19)
Journal based on events count and size (#19)

# Changes for v0.1.3 (2019-08-05)
Pass in s3 resource during instantiation (#18)
More meaningful name for event data offset (#17)
Turn on CI testing on travis (#16)

# Changes for v0.1.2 (2019-07-18)
Require twine for release (#15)
Update exception names (#14)

# Changes for v0.1.1 (2019-07-18)
Use twine to upload release to PyPi

# Changes for v0.1.0 (2018-06-20)
first FlashFlood release
