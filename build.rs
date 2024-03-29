fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("tests/schema")
        .file("tests/schema/test_all_types.capnp")
        .run()
        .expect("schema compiler command failed");
}
