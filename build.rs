use std::path::PathBuf;

/// Returns the `major.minor` release selected by the enabled Spark feature.
///
/// Cargo exposes enabled features to build scripts as `CARGO_FEATURE_<NAME>`,
/// so the set of known releases is whatever `[features]` declares - adding a
/// version never requires editing this file.
fn selected_release() -> String {
    let mut enabled: Vec<String> = std::env::vars()
        .filter_map(|(key, _)| {
            key.strip_prefix("CARGO_FEATURE_SPARK_")
                .map(|release| release.replace('_', "."))
        })
        .filter(|release| release.chars().all(|c| c.is_ascii_digit() || c == '.'))
        .collect();
    enabled.sort();

    match enabled.len() {
        1 => enabled.remove(0),
        0 => panic!(
            "no Spark version feature is enabled - select exactly one, e.g. \
             `--features spark-3-5`"
        ),
        // Cargo unions features across the dependency graph, so a second one
        // is usually pulled in by another crate rather than asked for here.
        _ => panic!(
            "Spark version features are mutually exclusive, but {} are enabled: {}. \
             Set `default-features = false` on whichever dependency enables the \
             one you do not want.",
            enabled.len(),
            enabled.join(", ")
        ),
    }
}

/// Locates the proto files for a release line.
///
/// Directories are keyed by `major.minor`; patch releases of a line overwrite
/// it in place, so upgrading within a line is a diff rather than a new tree.
fn proto_dir(release: &str) -> PathBuf {
    let dir = PathBuf::from(format!("protobuf/spark-{release}"));

    if !dir.is_dir() {
        panic!(
            "{} is missing - fetch it with `make protos SPARK_VERSION={release}.<patch>`",
            dir.display()
        );
    }

    dir
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let release = selected_release();
    let proto_dir = proto_dir(&release);

    // EXPORT TO RUNTIME:
    // This tells Cargo to set an environment variable when compiling the crate's source.
    println!("cargo:rustc-env=SPARK_VERSION={release}");

    // Cargo watches the whole tree, so a newly fetched release triggers a rebuild.
    println!("cargo:rerun-if-changed=protobuf");
    println!("cargo:rerun-if-changed=build.rs");

    let file_paths: Vec<PathBuf> = walkdir::WalkDir::new(&proto_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.into_path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("proto"))
        .collect();

    // Get protobuf compiler path and set environment variable
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    // SAFE in build.rs because this script runs single-threaded.
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }

    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .build_server(false)
        .build_client(true)
        .build_transport(true)
        .compile_protos(file_paths.as_ref(), &[proto_dir])?;

    Ok(())
}
