# V-FOR-WaTer data loader

This is a containerized Python tool to download data from [V-FOR-WaTer](https://portal.vforwater.de) following the [Tool Specification](https://vforwater.github.io/tool-specs/) for reusable research software using Docker.

## HYRAS example

You can follow the instructions in [`examples/hyras`](https://github.com/VForWaTer/tool_vforwater_loader/tree/main/examples/hyras)
to spin up a local docker cluster using docker compose, that will 
1. install PostgreSQL / PostGIS, 
2. create the necessary tables and pre-poluates them
3. downloads 10 years of HYRAS precipitation data
4. use one of three prepared hyrdo-MERIT catchments to clip the hyras data to the respective catchment