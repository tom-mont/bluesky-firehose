# Processing streams with quix streams

## Purpose of this repo

I would like to get to grips with Apache Flink. In order to do this I first need to have some data streaming through. I have worked with the [github firehose]() before but stumbled upon the [Bluesky firehose](https://docs.bsky.app/docs/advanced-guides/firehose) and thought it would be interesting to check out! I will again be using Quix streams with a bit of Claude-enabled wrangling to be able to ingest from the Websocket.

## Getting set up

```sudo quix pipeline up```

This gets kafka up and running. Fortunately, [quix](https://quix.io/get-started-with-quix-streams) obfuscates a lot of complexity here and this ends up being one simple command.

```python3 main.py```

We now begin producing events.

```kafkacat -b localhost:19092 -Ct bluesky-firehose | jq .```

Use kafka to "consume topic" (`-Ct`) from the broker ('-b') on port `19092`.

Piping into `jq .` turns all formats Json events nicely.

## Closing things off

```Ctrl+C```

Execute this command in the tab where `main.py` was running to stop the events from showing.

```quix pipeline down```

Kills the docker container where kafka is running.

## Notes

```CDEBUG:requests_sse.client:close```
Shows that the process is being closed off properly:

[Quix application docs](https://quix.io/docs/quix-streams/api-reference/application.html#applicationconfigcopy)

# Debugging

## Cannot stop containers using `sudo quix pipeline down`

If you receive a `permission denied` error when trying to stop the pipeline, run the following command:

```bash
sudo aa-remove-unknown
```

and re-run `quix pipeline down`.
