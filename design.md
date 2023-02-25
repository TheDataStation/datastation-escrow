# Design of Storage Modes

Information about Data Elements are stored in Data Station's database. They need 
a type, and an access_params, in addition to all the fields we currently require. We support
a fixed number of types: "file", "postgres", etc. Corresponding to types are access_params,
which are parameters needed to retrieve the DEs. For example, access_params for type
"file" can be a string, denoting name of the file; accessor for type "postgres"
can be an object, that includes name of DB, port, password, and a query.

In EPF file, developers will call get_accessible_DE, this is something 
provided by escrow_api.

In escrow_api, there will be a function called get_accessible_DE. Its implementation
is provided by the gatekeeper, because GK is in charge of brokering access to DEs.

In gatekeeper, GK knows which DEs are accessible from the policies. So all it needs
to do is calling the access method for those DEs based on the types, and pass in the
access_params. We support a fixed set of accessors, which are called get_DE_file,
get_DE_postgres, etc.