# Copernicus DEM example

This example incldues a installer service that loads an example Copernicus 30m-DEM for Baden-Württemberg to metacatalog.

To install the example, run:

```
cd examples/dem
docker compose up -d
```

Right now I can't figure out how to make the intsaller service wait until the db service is healthy.
Thus, after the thing is running, check the logs. The installer might have complained that the
db is not there. Then, invoke the installer manually, until that issue is resolved.

```
docker compose run --rm installer
```

The same thing applies to the actual examples. I can't figure out a way, how they could `depend_on: installer`,
in a way, that they wait until the installer has *finished*. 
Hence, their default command is overwritten to just echo the command that one would need to actually run the tool.

Long story short: To run one of the examples, with the `/examples/dem` compose cluster running, you can:

```
cd examples/dem
docker compose run --rm de210080_loader python run.py
```
