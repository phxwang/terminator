use tonic_build::configure;

fn main() {
    // 注意：文件路径不包src也可以编译，但会导致本crate每次都会被编译
    configure().compile(&[
        "src/extra.proto",
        "src/dummy.proto",
    ], &["src"]).unwrap();
}