use std::fs;


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = fs::read_dir("./protobuf/spark/connect/")?;

    let mut file_paths: Vec<String> = vec![];

    for file in files {
        let entry = file?.path();
        file_paths.push(entry.to_str().unwrap().to_string());
    }

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
        .compile_protos(file_paths.as_ref(), &["./protobuf/".to_string()])?;

    Ok(())
}