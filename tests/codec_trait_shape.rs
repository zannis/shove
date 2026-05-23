//! Verifies the public `Codec` trait shape. Compile-only — if this file
//! builds, the trait is exposed in the documented form.

use shove::codec::Codec;
use shove::error::Result;

struct DummyCodec;
impl Codec<Vec<u8>> for DummyCodec {
    const NAME: &'static str = "dummy";
    fn encode(value: &Vec<u8>) -> Result<Vec<u8>> {
        Ok(value.clone())
    }
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

#[test]
fn dummy_codec_name() {
    assert_eq!(<DummyCodec as Codec<Vec<u8>>>::NAME, "dummy");
}
