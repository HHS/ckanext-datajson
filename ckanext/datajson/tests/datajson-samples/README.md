# Data.json Examples
This contains the various examples for DCAT-US implementations.

### Geospatial Full Metadata Link Example
You can see this example at [geospatial.data.json](./geospatial.data.json).
We are examining 2 ways of guidance for adding an original full metadata link:

1. The first example adds a new `Dataset Distribution Field` for the link. The requirements are to utilize the `conformsTo` field to specify what metadata flavor is used, so that downstream users (ie Geoplatform) can examine and decide if this metadata is ingestible/usable. This is easily discoverable and usable by both machines and users.

1. The second example utilizes the current Dataset Distribution and adds the `describedBy` and `describedByType` fields to add the full metadata link. Since the `describedByType` field is limited to a well-defined list, we cannot use the metadata flavor link in the above example and have to specify simply `text/xml` (or some other MIME type). This approach does not display the link on the main dataset page (though it would still be availabe on the resource page), which may be a pro or a con.