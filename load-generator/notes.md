# Next steps

- add cli argument to choose the connection string or at least configure host, port
- add "https://github.com/twpayne/pgx-geos" library to support geometric types and WKT

  - this will allow to pass geometric objects as arguments to pgx.Conn.Exec method
  - automatic sanitization

- remove the loading of templating from schemas, move everything into the load generator code as it is strongly correlated
- add the configuration for mobilitydb
- make sure the logging works correctly
- add randomized querying functionality
- test in the cloud
