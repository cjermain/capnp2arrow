fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src/schema")
        .file("src/schema/point.capnp")
        .output_path("src/schema")
        .run()
        .expect("schema compiler command failed");
}
