# mr-graph

Utilities to build a weighted co-occurrence graph of tracks that share Spotify playlists.

## Project layout

```
mr_graph/           Python package with the reusable graph logic
  backends.py       Backend implementations (Neo4j, NetworkX)
  builder.py        CSV ingestion and edge computation helpers
  cli.py            Command-line interface utilities
main.py             Thin entry-point forwarding to `mr_graph.cli`
data/               Example CSV source data
```


## Retrieval helpers

Use `mr_graph.retrieval.get_related_tracks(track_id, backend, k)` to retrieve the top k connected tracks sorted by the co-occurrence weight. A placeholder `get_related_tracks_for_multiple` is available for future multi-track queries.

