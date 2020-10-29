# Data.json Examples
This contains the various examples for DCAT-US implementations.

### Geospatial Full Metadata Link Example
You can see this example at [geospatial.data.json](./geospatial.data.json).
We are examining 2 ways of guidance for adding an original full metadata link:

1. The first example adds a new `Dataset Distribution Field` for the link. The requirements are to utilize the `conformsTo` field to specify what metadata flavor is used, so that downstream users (ie Geoplatform) can examine and decide if this metadata is ingestible/usable

1. The second example utilizes the current Dataset Distribution add adds the `describedBy` and `describedByType` fields to add the full metadat link. Since the `describedByType` field is limited to a well-defined list, we cannot use the metadata flavor type and have to specify simply `xml`. This also hides the link from general users, which may be a pro or a con.