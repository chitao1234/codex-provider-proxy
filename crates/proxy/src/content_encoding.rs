use std::io::{self, Read};

use axum::http::{header, HeaderMap};
use flate2::read::{DeflateDecoder, GzDecoder, ZlibDecoder};

const MAX_DECODED_LOG_BODY_BYTES: u64 = 64 * 1024 * 1024;

pub fn decode_content_encoded_body(headers: &HeaderMap, body: &[u8]) -> io::Result<Vec<u8>> {
    let encodings = content_encodings(headers)?;
    decode_content_encodings(body, &encodings)
}

pub fn content_encodings(headers: &HeaderMap) -> io::Result<Vec<String>> {
    let mut encodings = Vec::new();

    for value in headers.get_all(header::CONTENT_ENCODING) {
        let value = value.to_str().map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid Content-Encoding header: {err}"),
            )
        })?;
        for encoding in value.split(',') {
            let encoding = encoding.trim();
            if encoding.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty content encoding",
                ));
            }
            encodings.push(encoding.to_ascii_lowercase());
        }
    }

    Ok(encodings)
}

pub fn decode_content_encodings(body: &[u8], encodings: &[String]) -> io::Result<Vec<u8>> {
    let mut decoded = body.to_vec();

    // Content codings are applied in header order and therefore decoded in reverse order.
    for encoding in encodings.iter().rev() {
        decoded = decode_one_content_encoding(decoded, encoding)?;
    }

    Ok(decoded)
}

fn decode_one_content_encoding(body: Vec<u8>, encoding: &str) -> io::Result<Vec<u8>> {
    match encoding {
        "identity" => Ok(body),
        "gzip" | "x-gzip" => read_decoded_limited(GzDecoder::new(body.as_slice())),
        "deflate" => decode_deflate(&body),
        "br" => read_decoded_limited(brotli::Decompressor::new(body.as_slice(), 4096)),
        "zstd" => {
            let decoder = zstd::stream::read::Decoder::new(body.as_slice())?;
            read_decoded_limited(decoder)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported content encoding {encoding:?}"),
        )),
    }
}

fn decode_deflate(body: &[u8]) -> io::Result<Vec<u8>> {
    match read_decoded_limited(ZlibDecoder::new(body)) {
        Ok(decoded) => Ok(decoded),
        Err(zlib_err) => read_decoded_limited(DeflateDecoder::new(body)).map_err(|raw_err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "failed to decode deflate content with zlib ({zlib_err}) or raw deflate ({raw_err})"
                ),
            )
        }),
    }
}

fn read_decoded_limited<R: Read>(reader: R) -> io::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut reader = reader.take(MAX_DECODED_LOG_BODY_BYTES.saturating_add(1));
    reader.read_to_end(&mut decoded)?;
    if decoded.len() as u64 > MAX_DECODED_LOG_BODY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "decoded content exceeds the {MAX_DECODED_LOG_BODY_BYTES}-byte log reconstruction limit"
            ),
        ));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use axum::http::{header, HeaderMap};
    use flate2::{
        write::{DeflateEncoder, GzEncoder, ZlibEncoder},
        Compression,
    };

    use super::{decode_content_encoded_body, decode_content_encodings};

    #[test]
    fn decodes_zstd_content() {
        let payload = br#"{"message":"decoded"}"#;
        let encoded = zstd::stream::encode_all(payload.as_slice(), 3).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_ENCODING, "zstd".parse().unwrap());

        assert_eq!(
            decode_content_encoded_body(&headers, &encoded).unwrap(),
            payload
        );
    }

    #[test]
    fn decodes_gzip_deflate_and_brotli_content() {
        let payload = br#"{"message":"decoded"}"#;

        let mut gzip_encoder = GzEncoder::new(Vec::new(), Compression::default());
        gzip_encoder.write_all(payload).unwrap();
        let gzip = gzip_encoder.finish().unwrap();
        assert_eq!(
            decode_content_encodings(&gzip, &["gzip".to_string()]).unwrap(),
            payload
        );

        let mut zlib_encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        zlib_encoder.write_all(payload).unwrap();
        let zlib_deflate = zlib_encoder.finish().unwrap();
        assert_eq!(
            decode_content_encodings(&zlib_deflate, &["deflate".to_string()]).unwrap(),
            payload
        );

        let mut raw_deflate_encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        raw_deflate_encoder.write_all(payload).unwrap();
        let raw_deflate = raw_deflate_encoder.finish().unwrap();
        assert_eq!(
            decode_content_encodings(&raw_deflate, &["deflate".to_string()]).unwrap(),
            payload
        );

        let mut brotli_encoder = brotli::CompressorWriter::new(Vec::new(), 4096, 5, 22);
        brotli_encoder.write_all(payload).unwrap();
        let brotli = brotli_encoder.into_inner();
        assert_eq!(
            decode_content_encodings(&brotli, &["br".to_string()]).unwrap(),
            payload
        );
    }

    #[test]
    fn decodes_stacked_content_in_reverse_order() {
        let payload = b"decoded";
        let once_encoded = zstd::stream::encode_all(payload.as_slice(), 3).unwrap();
        let twice_encoded = zstd::stream::encode_all(once_encoded.as_slice(), 3).unwrap();

        assert_eq!(
            decode_content_encodings(&twice_encoded, &["zstd".to_string(), "zstd".to_string()])
                .unwrap(),
            payload
        );
    }
}
