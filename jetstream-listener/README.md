# jetstream-listener

This is a standalone Go service. It ingests the [Bluesky Jetstream](https://github.com/bluesky-social/jetstream) and discover mentions of the @doj47.com account.

Those mentions are queued in the `post_mentions` table for future processing.

Ultimately, the service builds a SQLite DB that contains all of the Bluesky data needed
to operate https://doj47.com.

The service is meant to be interruptible - it resumes processing where it left off, so long
as it is restarted before the Jetstream has moved on. The Jetstream generally has about
30 hours of data in it, so we can tolerate up to a day of downtime.

# DB schema

## `post_mentions`

We subscribe to the `app.bsky.feed.post` collection.

When someone mentions `@doj47.com`, the underlying post has a reference to that
account's DID: `did:plc:dcclyrbpqvapa3f44zm4w4zq`.

Those mentions get logged in `post_mentions` with `processed` set to `0`.
