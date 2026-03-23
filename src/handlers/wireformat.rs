use crate::handlers::protobuf::zigzag_decode;
use crate::models::WireFormatHeader;
use anyhow::{Error, anyhow};

impl WireFormatHeader {
    fn decode_varint(bytes: &[u8], pos: usize) -> Result<(u64, usize), Error> {
        let mut shift = 0u32;
        let mut result: u64 = 0;
        let mut i = pos;
        while i < bytes.len() {
            let b = bytes[i];
            let chunk = (b & 0x7F) as u64;
            result |= chunk << shift;
            shift += 7;
            i += 1;
            // MSB not set -> end of varint
            if b & 0x80 == 0 {
                return Ok((result, i - pos));
            }

            if shift >= 64 {
                return Err(anyhow!("Varint trop long (>= 64 bytes)"));
            }
        }
        Err(anyhow!("Unexpected EOF while decoding varint"))
    }

    fn decode_varint_as_signed(bytes: &[u8], pos: usize) -> Result<(i64, usize), Error> {
        let (v, consumed) = Self::decode_varint(bytes, pos)?;

        Ok((zigzag_decode(v), consumed))
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 6 {
            return Err(anyhow!("Message trop court pour l'en-tête Wire Format"));
        }

        let magic_byte = bytes[0];
        if magic_byte != 0 {
            return Err(anyhow!("Magic byte invalide: {}", magic_byte));
        }

        let schema_id = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);

        let mut pos = 5usize;

        let (size_signed, consumed) = Self::decode_varint_as_signed(bytes, pos)?;

        pos += consumed;
        if size_signed < 0 {
            return Err(anyhow!("Taille d'index négative invalide: {}", size_signed));
        }
        let size = size_signed as usize;

        let mut indexes = Vec::new();

        if size == 0 {
            // size==0 -> [0]
            indexes.push(0);
        } else {
            for _ in 0..size {
                if pos >= bytes.len() {
                    return Err(anyhow!("EOF while reading message indexes"));
                }
                let (idx, c) = Self::decode_varint_as_signed(bytes, pos)?;
                pos += c;
                indexes.push(idx);
            }
        }

        Ok(WireFormatHeader {
            magic_byte,
            schema_id,
            message_indexes: indexes,
            position: pos,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_format_header_from_bytes() {
        // Payload : Magic(0), SchemaID(123), SizeIndex(1), Index(0)
        // En zig-zag : 1 => 2, 0 => 0.
        let mut bytes = vec![0u8]; // Magic
        bytes.extend_from_slice(&123i32.to_be_bytes()); // Schema ID
        bytes.push(2); // Size index = 1 (zigzag)
        bytes.push(0); // Index 0 = 0 (zigzag)

        let header = WireFormatHeader::from_bytes(&bytes).expect("décodage impossibled");
        assert_eq!(header.magic_byte, 0);
        assert_eq!(header.schema_id, 123);
        assert_eq!(header.message_indexes, vec![0]);
        assert_eq!(header.position, 7);
    }

    #[test]
    fn test_decode_varint_basic() {
        // varint simple (valeur 150)
        let bytes = vec![0x96, 0x01];
        let (val, consumed) = WireFormatHeader::decode_varint(&bytes, 0).unwrap();
        assert_eq!(val, 150);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_invalid_magic_byte() {
        let bytes = vec![1, 0, 0, 0, 5, 2, 0]; // Magic byte = 1 (invalide)
        let result = WireFormatHeader::from_bytes(&bytes);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Magic byte invalide")
        );
    }

    #[test]
    fn test_zigzag_logic() {
        assert_eq!(zigzag_decode(0), 0);
        assert_eq!(zigzag_decode(1), 0);
        assert_eq!(zigzag_decode(2), 1);
        assert_eq!(zigzag_decode(3), -1);
    }
}
