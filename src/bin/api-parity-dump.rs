use ::spark_connect;
// use api_parity_rs;

fn main() -> std::io::Result<()> {
    api_parity_rs::dump_to_writer(
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        std::io::stdout(),
    )
}