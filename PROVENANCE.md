# Source provenance

The transport package derives from the ShardCast 0.3.2 source snapshot used by
`goncalorafaria/prime-rl`. It includes HTTP/in-memory distribution, optional UCXX,
manifest verification, streaming, and selective CPU-buffer transfer work developed
alongside that integration. Original authorship and version metadata are retained.
The package/import/CLI/environment namespace is LiteCast.

The `litecast.registry` modules and registry tests were extracted from the PrimeRL
fork's LiteCast integration, including measured parallel transfers, publisher
leases, SQLite head discovery, and reloadable multi-publisher middle supervision.
They have no dependency on PrimeRL. Compatible registry keys and adapter wire names
are intentionally preserved.

Build outputs, bytecode, installed environments, model weights, credentials, and
experiment artifacts are excluded. This PR imports the implementation into its
standalone repository; it is not a PyPI release.
