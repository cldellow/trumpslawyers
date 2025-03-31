# jetstream-listener

This is a standalone Go service. It ingests the [Bluesky Jetstream](https://github.com/bluesky-social/jetstream) and discover mentions of the @doj47.com account.

Those mentions are queued in the `post_mentions` table for future processing.

Ultimately, the service builds a SQLite DB that contains all of the Bluesky data needed
to operate https://doj47.com.

The service is meant to be interruptible - it resumes processing where it left off, so long
as it is restarted before the Jetstream has moved on. The Jetstream generally has about
30 hours of data in it, so we can tolerate up to a day of downtime.

# Dev

Run with `go run *.go`.

A `doj47.sqlite` DB will be created if needed.

If you're testing locally, you might want to create your own account and update the DID
that is used, rather than testing against the real doj47.com DID.

# DB schema

## `post_mentions`

We subscribe to the `app.bsky.feed.post` collection.

When someone mentions `@doj47.com`, the underlying post has a reference to that
account's DID: `did:plc:dcclyrbpqvapa3f44zm4w4zq`.

Those mentions get logged in `post_mentions` with `processed` set to `0`.

## `post`

This is a mirror of Bluesky posts.

Basically, it's the output of https://docs.bsky.app/docs/api/app-bsky-feed-get-posts.

## `post_queue`

This is a queue of post IDs to be fetched.

Posts will get dumped into `post`. We track when we last fetched a post.

We are willing to re-fetch posts on some frequency, e.g. to capture new like/RT counts.
We try to be a good citizen -- if a post hasn't changed much since we last fetched it,
we'll back off on our refetch attempts.
