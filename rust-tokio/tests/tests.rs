use testcontainers::images::generic::GenericImage;

#[test]
fn scenario1() {
    let client = testcontainers::clients::Cli::docker();
    let container = client
        .run(GenericImage::new("ghcr.io/jamesward/easyracer", "latest").with_exposed_port(8080));
}
