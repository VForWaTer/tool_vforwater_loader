# tool_template_python

[![Docker Image CI](https://github.com/VForWaTer/tool_vforwater_loader/actions/workflows/docker-image.yml/badge.svg)](https://github.com/VForWaTer/tool_vforwater_loader/actions/workflows/docker-image.yml)

This is a containerized Python tool to download data from [V-FOR-WaTer](https://portal.vforwater.de) following the [Tool Specification](https://vforwater.github.io/tool-specs/) for reusable research software using Docker.

## Description

The parameters and connection will be exaplained here.

## Structure

This container implements a common file structure inside container to load inputs and outputs of the 
tool. it shares this structures with the [Python template](https://github.com/vforwater/tool_template_python), 
[R template](https://github.com/vforwater/tool_template_r),
[NodeJS template](https://github.com/vforwater/tool_template_node) and [Octave template](https://github.com/vforwater/tool_template_octave), 
but can be mimiced in any container.

Each container needs at least the following structure:

```
/
|- in/
|  |- inputs.json
|- out/
|  |- ...
|- src/
|  |- tool.yml
|  |- run.py
```

* `inputs.json` are parameters. Whichever framework runs the container, this is how parameters are passed.
* `tool.yml` is the tool specification. It contains metadata about the scope of the tool, the number of endpoints (functions) and their parameters
* `run.py` is the tool itself, or a Python script that handles the execution. It has to capture all outputs and either `print` them to console or create files in `/out`

## How to build the image?

You can build the image from within the root of this repo by
```
docker build -t tbr_vfw_loder .
```

Alternatively, the contained `.github/workflows/docker-image.yml` will build the image for you 
on new releases on Github. You need to change the target repository in the aforementioned yaml.

## How to run?

This template installs the json2args python package to parse the parameters in the `/in/inputs.json`. 
To invoke the docker container directly run something similar to:
```
docker run --rm -it -v /path/to/local/in:/in -v /path/to/local/out:/out tbr_vfw_loader
```

Then, the output will be in your local out and based on your local input folder. Stdout and Stderr are also connected to the host.

