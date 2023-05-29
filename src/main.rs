extern crate capnp;
extern crate core;

pub mod point_capnp {
    include!(concat!(env!("OUT_DIR"), "/point_capnp.rs"));
}

pub mod point_demo {
    use crate::point_capnp::point;
    use capnp::serialize_packed;

    pub fn write_to_stream() -> ::capnp::Result<()> {
        let mut message = ::capnp::message::Builder::new_default();

        let mut demo_point = message.init_root::<point::Builder>();

        demo_point.set_x(5_f32);
        demo_point.set_y(10_f32);

        serialize_packed::write_message(&mut ::std::io::stdout(), &message)
    }
}

fn main() {
    let _ = point_demo::write_to_stream();
}
