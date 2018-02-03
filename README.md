# Development

## Setting the `PYTHONPATH`

Set the `PYTHONPATH` variable for module importing. To do this, you need to work in the command line.

### Mac OS X / Linux

    $ cd nyc-green-taxi-map-visualization
    $ export PYTHONPATH="$(pwd)"

## Working in a Virtual Environment \[RECOMMENDED\]

### Using vanilla `virtualenv`

### Using `virtualenvwrapper` \[RECOMMENDED\]

## Installing pip packages

    $ pip install -U pip
    $ pip install -r requirements_dev.txt

## Updating requirements \& dependencies

Make sure `pip-tools` has already been installed. If you ran `pip install -r requirements_dev.txt`, then `pip-tools==1.11.0` should be installed.

_NOTE: Never directly update `requirements.txt` or `requirements_dev.txt`._

Instead, use `pip-compile`

    $ pip-compile requirements.in
    $ pip-compile requirements_dev.in
