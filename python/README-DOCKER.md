# Running Tests Using Docker

## Bring up Containers

run `docker-compose up -d` in this directory.

## Run Tests

`docker exec -it redis-in-action-python python ch0*_listing_source.py`

## Why Using `network_mode: "host"`?

Because Redis host defaults to `localhost` in every `redis.Redis()`, you would otherwise have to specify `host` in every occurrence.