# Data.json Examples
This contains the various examples for DCAT-US implementations.

### Geospatial Full Metadata Link Example
You can see this example at [geospatial.data.json](./geospatial.data.json).

The examples adds a new `Dataset Distribution Field` for the link. 
The requirements are to utilize the `conformsTo` field to specify what metadata flavor is used, 
so that downstream users (ie Geoplatform) can examine and decide if this metadata is ingestible/usable. 
This is easily discoverable and usable by both machines and users.

Best practice also includes the following:

- Use the `downloadURL` field as long as the url is a direct access link to the file
- Include an accurate [mediaType](https://resources.data.gov/resources/dcat-us/#distribution-mediaType) and [format](https://resources.data.gov/resources/dcat-us/#distribution-format) (probably `text/xml` and `XML`, respectively)
- Utilize a consistent title and description so that your users will quickly recognize the resource (and be able to skip over or dig into the details as appropriate)
- Include the [geospatial theme](./geospatial.data.json#L24) so that your dataset will be marked as geospatial