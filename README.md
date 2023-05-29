# capnp2arrow

## Setup

```
rustup override set 1.65.0  # set Rust version to newer
sudo apt install capnproto  # install compiler
```

Generate an id: `capnp id`

## Demo

```
cargo run | capnp decode ./src/schema/point.capnp Point --packed
```

## Generating examples

```
echo '{"x": 4, "y": 8}' | capnp convert json:packed ./src/schema/point.capnp Point > point.bin
```

## References

1. Reflection based `Debug` implementation: https://github.com/capnproto/capnproto-rust/blob/f7c86befe11b27f33c2a45957d402abff2b9e347/capnp/src/stringify.rs

2. Reflection based example: https://github.com/capnproto/capnproto-rust/blob/master/example/fill_random_values/src/lib.rs

3. Cap'N Proto `TypeVariant`: https://docs.rs/capnp/latest/capnp/introspect/enum.TypeVariant.html

4. Arrow2 `DataTypes`: https://docs.rs/arrow2/latest/arrow2/datatypes/enum.DataType.html

5. Cap'N Proto Language Reference: https://capnproto.org/language.html
