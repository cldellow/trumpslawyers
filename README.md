# jetstream-listener

This is a standalone Go service. It ingests the [Bluesky Jetstream](https://github.com/bluesky-social/jetstream) to discover tweets
that should be included on https://doj47.com.

It's pretty dumb! It archives any posts liked or made by people in the `watched_dids` table.

Those IDs are queued in the `post_queue` table for future processing.

The service is meant to be interruptible - it resumes processing where it left off, so long
as it is restarted before the Jetstream has moved on. The Jetstream generally has about
30 hours of data in it, so we can tolerate up to a day of downtime.

The code is likely a clusterfuck - I vibe-coded this with ChatGPT, because I don't actually program go professionally.

# Dev

Run with `go run *.go`.

A `doj47.sqlite` DB will be created if needed.

If you're testing locally, you might want to create your own account and update the DID
that is used, rather than testing against the real doj47.com DID.

# DB schema

## `post`

This is a mirror of Bluesky posts.

Basically, it's the output of https://docs.bsky.app/docs/api/app-bsky-feed-get-posts.

## `post_queue`

This is a queue of post IDs to be fetched, populated by us subscribing to the
`app.bsky.feed.post` collection and the `app.bsky.feed.like` collection.

Posts will get dumped into `post`. We track when we last fetched a post.

We are willing to re-fetch posts on some frequency, e.g. to capture new like/RT counts.
We try to be a good citizen -- if a post hasn't changed much since we last fetched it,
we'll back off on our refetch attempts.

## `watched_dids`

These are people whose likes or posts we'll collect automatically.

You must restart the service if you add/remove entries.

> Tip:
>
> To translate a handle to a DID, use https://bsky.social/xrpc/com.atproto.identity.resolveHandle?handle=cldellow.com
