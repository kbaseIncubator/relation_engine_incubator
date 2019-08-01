# Relation Engine Incubator

This repo contains Relation Enginer (RE) scripts, loaders, etc. that are in the experimental stage.
They may be promoted into one of the [RE repos](https://kbase.github.io/) later.

Each script / loader / etc. should have its own documentation, either in a readme file with a name
keyed to the script or embedded in the script.

To run tests, arangodb must be running locally on the default port without credentials.
Then from the repository root:
```
export PYTHONPATH=$(pwd); pytest
```