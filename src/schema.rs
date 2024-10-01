#[cfg(feature = "default")]
include!(concat!(env!("OUT_DIR"), "/generated_schema.rs"));

#[cfg(feature = "doc-only")]
pub static SUPPORTED_SCHEMAS: &'static [&'static str] = &[];
