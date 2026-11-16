# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

**Name:** Wilson Fredbeck
**Date:** 2026-11-16

## 1. The source
- Source and URL: Bluesky posts via Jetstream, `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post`
- Method (WebSocket push, REST polling or REST history pull): WebSocket push
- Account, key or User-Agent needed: none (no subscribe message needed; the filter is in the URL)
- Limits I know about (rate limits, paging, reconnects): no rate limit, but the stream is fast (~40 posts/s), so the producer filters to posts only through `wantedCollections`. The connection can drop, so the producer reconnects after a 5 s pause. Jetstream can replay a limited window with `?cursor=<time_us>`.

## 2. One real message
```json
{
  "did": "did:plc:cxbrzprureskhbht2v7jcwho",
  "time_us": 1794832450653413,
  "cursor": 27243604296,
  "kind": "commit",
  "commit": {
    "rev": "3n2gjzdv6ua2r",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3n2gjzdrl6k2l",
    "record": {
      "$type": "app.bsky.feed.post",
      "createdAt": "2026-11-16T12:34:09.914Z",
      "embed": {"$type": "app.bsky.embed.record", "record": {"cid": "bafyrei…", "uri": "at://…"}},
      "langs": ["en"],
      "text": "Yes, lets imagine together…"
    },
    "cid": "bafyreibsnhvht3ksnhk7fe64hrlw4ahkmspbnhngquphr2luvc443ni4yy"
  }
}
```

## 3. Volume maths
- Measured rate (from the probe): 39.64 messages per second (2,379 messages in 60 s, 16 Nov, 12:34 UTC)
- Live capture needed for 50,000: about 1,261 s (21 min)
- History / backfill available? How many days? A limited replay with `?cursor=<time_us>` (hours, not days). Not needed at this rate.
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): I am catching up and collecting on Mon 16 Nov. I will run the producer and lander together for about 45 minutes, roughly double the time needed, to land about 100,000 raw records.

## 4. Topic design
- Topic name: `bluesky_posts` (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: `did` (the account that posted)
- Why this key (ordering, spread across partitions): every event from one account goes to the same partition, in order, so a post's create and later delete stay in sequence. Bluesky has millions of accounts, so the keys spread evenly over the 3 partitions with no hot partition.

## 5. Duplicates
- What makes two records **the same Kafka record**? The same `kafka_partition` + `kafka_offset`. This happens when the lander crashes after writing a file but before storing offsets (card D1).
- What makes two records **the same thing**? The same `did` + `commit.rkey`: one account's post. It can arrive twice with new offsets if the producer replays with `?cursor=` (card D5).
- If the same thing can arrive in several versions, which one do I keep? The latest event by `time_us` (then highest offset). A post that was created and then deleted keeps its `delete` version, and marts count posts with `operation = 'create'`.

## 6. Research question (draft)
- Which languages post the most on Bluesky, and which of them have the highest share of replies (vs. original posts)? A comparison by `langs[0]` with a share per language, using `commit.record.reply` to mark replies.

## 7. Self-check (checkpoint A)
- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
