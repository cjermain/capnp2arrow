# capnp2arrow

This is a work-in-progress demonstration of reading a series of Cap'N Proto messages into Arrow. The dynamic value Reader is used to flexibly traverse arbitrary schemas, allowing the library to be schema-agnostic.

## Setup

```
# rustup override set 1.69.0  # run a recent Rust version
sudo apt install capnproto  # install compiler
```

Generate an id: `capnp id`

## Demo

```
echo '{"x": -2, "y": 5}' | capnp convert json:packed ./src/schema/point.capnp Point | cargo run
```

```
echo '{"x": 4, "y": 8}' | capnp convert json:packed ./src/schema/point.capnp Point | cat - <(echo "") >> points.bin
echo '{"x": 5, "y": 1}' | capnp convert json:packed ./src/schema/point.capnp Point | cat - <(echo "") >> points.bin
echo '{"x": 3, "y": -7}' | capnp convert json:packed ./src/schema/point.capnp Point | cat - <(echo "") >> points.bin
echo '{"x": -2, "y": 4}' | capnp convert json:packed ./src/schema/point.capnp Point | cat - <(echo "") >> points.bin
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
