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
