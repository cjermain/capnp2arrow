fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src/schema")
        .file("src/schema/point.capnp")
        .run()
        .expect("schema compiler command failed");
}
