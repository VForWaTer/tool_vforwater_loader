# hyras example

This example incldues a installer service that loads HYRAS data to metacatalog.
The docker-compose.yml sets relative paths to the main repository, to build the 
Dockerfile in the root context and also shares the volumes with all other examples
and the root project.

**Therefore you need to use `docker compose` in the root directory!**

```
docker compose -f examples/hyras/docker-compose.yml up -d
```
