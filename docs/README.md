# Chr.Avro documentation site

This directory contains the source code for https://engineering.chrobinson.com/dotnet-avro/.

## Building and running

The site is generated by [MkDocs](https://www.mkdocs.org/). To develop the site, you’ll need Python 3.11 or better installed.

To run the site locally:

```shell
# install dependencies:
pip install mkdocs mkdocs-material

# start the dev server:
mkdocs serve
```

## Automated releases

The [Docs workflow](../.github/workflows/docs.yml) automatically builds and deploys the site whenever changes are pushed to this directory or the library source directory.
