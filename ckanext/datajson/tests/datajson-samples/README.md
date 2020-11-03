# Data.json Examples
This contains the various examples for DCAT-US implementations.

### Geospatial Full Metadata Link Example
You can see this example at [geospatial.data.json](./geospatial.data.json).

The examples adds a new `Dataset Distribution Field` for the link. 
The requirements are to utilize the `conformsTo` field to specify what metadata flavor is used, 
so that downstream users (ie Geoplatform) can examine and decide if this metadata is ingestible/usable. 
This is easily discoverable and usable by both machines and users.

