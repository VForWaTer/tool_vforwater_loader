# hyras example

This example incldues a installer service that loads HYRAS data to metacatalog.
The docker-compose.yml sets relative paths to the main repository, to build the 
Dockerfile in the root context and also shares the volumes with all other examples
and the root project.

**Therefore you need to use `docker compose` in the root directory!**

```
docker compose -f examples/hyras/docker-compose.yml up -d
```

Right now I can't figure out how to make the intsaller service wait until the db service is healthy.
Thus, after the thing is running, check the logs. The installer might have complained that the
db is not there. Then, invoke the installer manually, until that issue is resolved.

```
docker compose -f examples/hyras/docker-compose.yml run --rm installer
```

The same thing applies to the actual examples. I can't figure out a way, how they could `depend_on: installer`,
in a way, that they wait until the installer has *finished*. 
Hence, their default command is overwritten to just echo the command that one would need to actually run the tool.

Long story short: To run one of the examples, with the `/examples/hyras` compose cluster running, you can:

```
cd examples/hyras
docker compose run --rm de410890_loader python run.py
```
