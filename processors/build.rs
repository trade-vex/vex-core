fn main() {
    prost_build::compile_protos(&["src/protos/trading.proto"], &["src/protos/"]).unwrap();
}

