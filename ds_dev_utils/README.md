# Data Station Dev Utils

Scripts to support a function developer on Data Station.

## Getting Started
Write functions that act on data, then save them at some directory `/example/function/directory/functions.py`.

Create a new `ds_docker` class and provide it with the inputs:
```python
session = ds_docker(
    '/example/function/directory/functions.py',
    "connector_file",
    '/my_data/directory',
    "image"
)
```

Then call the function in the `functions.py` connector with:

```python
session.network_run("read_file", "/mnt/data/hi.txt")
```

Finally, clean up the network and containers.

```python
session.network_remove()
session.stop_and_prune()
```


## Installation
Install Docker. All other dependencies should be python built-in.

## Under the Hood
Read the [design doc](function_approval_design_doc.md).