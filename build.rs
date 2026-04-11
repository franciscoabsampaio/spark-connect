use walkdir;


fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Determine the version safely using an if-else chain.
    // This prevents compilation errors if multiple features are accidentally enabled.
    let spark_version = if cfg!(feature = "spark-3-4") {
        "3.4.0"
    } else if cfg!(feature = "spark-3-5") {
        "3.5.7"
    } else {
        panic!("Please select a Spark version feature.");
    };

    // 2. EXPORT TO RUNTIME: 
    // This tells Cargo to set an environment variable when compiling your crate's source code.
    println!("cargo:rustc-env=SPARK_VERSION={}", spark_version);

    // Tell cargo to re-run the build script if the protobuf files or build script change
    println!("cargo:rerun-if-changed=protobuf/spark-{spark_version}");
    println!("cargo:rerun-if-changed=build.rs");
    
    let proto_dir = format!("./protobuf/spark-{spark_version}");

    let file_paths: Vec<String> = walkdir::WalkDir::new(&proto_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|e| e.to_str()) == Some("proto"))
        .filter_map(|e| e.path().to_str().map(|s| s.to_string()))
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