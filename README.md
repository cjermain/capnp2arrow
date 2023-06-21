# capnp2arrow

This is a work-in-progress demonstration of reading a series of Cap'N Proto messages into Arrow. The dynamic value Reader is used to flexibly traverse arbitrary schemas, allowing the library to be schema-agnostic.

## Setup

```
# rustup override set 1.69.0  # run a recent Rust version
sudo apt install capnproto  # install compiler
```

Generate an id: `capnp id`

## Demo

Create a JSON Lines file with a new-line separated list of points:
```
cat << EOF > points.jsonl
{"values": [{"x": 0, "y": 1}, {"x": -1, "y": 2}]}
{"values": [{"x": 0, "y": 0}]}
{"values": [{"x": -2, "y": 3}]}
EOF
```

Convert the JSONL to binary Cap'N Proto messages based on the schema:
```
cat points.jsonl | capnp convert json:binary ./src/schema/point.capnp Points > points.bin
```

Run the binary messages through the demo:
```
cat points.bin | cargo run
```

## Tests

```
cargo test
```

## References

1. Reflection based `Debug` implementation: https://github.com/capnproto/capnproto-rust/blob/f7c86befe11b27f33c2a45957d402abff2b9e347/capnp/src/stringify.rs

2. Reflection based example: https://github.com/capnproto/capnproto-rust/blob/master/example/fill_random_values/src/lib.rs

3. Cap'N Proto `TypeVariant`: https://docs.rs/capnp/latest/capnp/introspect/enum.TypeVariant.html

4. Arrow2 `DataTypes`: https://docs.rs/arrow2/latest/arrow2/datatypes/enum.DataType.html

5. Cap'N Proto Language Reference: https://capnproto.org/language.html

6. Cap'N Proto test schema: https://github.com/capnproto/capnproto/blob/master/c%2B%2B/src/capnp/test.capnp

7. Cap'N Proto test JSON: https://github.com/capnproto/capnproto/blob/master/c%2B%2B/src/capnp/testdata/pretty.json
