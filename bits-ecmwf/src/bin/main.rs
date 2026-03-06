// bits-ecmwf: the BITS service with ECMWF actions pre-loaded.
//
// bits-ecmwf actions (match, has_license, metkit_expansion, mars_destination,
// dss_destination) are registered automatically at startup via inventory.
// No explicit init call is needed — just run and pass a config.

#[tokio::main]
async fn main() {
    bits::cli::run().await;
}
