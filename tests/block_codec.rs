use pique::block::BlockReader;
use pique::format::{BLOCK_CODEC_VERSION, FORMAT_VERSION};
use pique::{ReaderError, SegmentReader, SegmentWriter, SegmentWriterOptions};

fn build(compressed: bool) -> Vec<u8> {
    let options = SegmentWriterOptions {
        block_size: 4096,
        ..Default::default()
    };
    let mut writer = if compressed {
        SegmentWriter::new_compressed(options)
    } else {
        SegmentWriter::new(options)
    };
    let value = b"s3://lodestar-sample/root/repo=owner%2Frepo/generation/file.parquet".repeat(4);
    for index in 0..200 {
        writer
            .add(format!("id-{index:03}").as_bytes(), &value)
            .unwrap();
    }
    writer.add(b"z-raw", b"r").unwrap();
    writer.finish().unwrap().data
}

#[test]
fn both_formats_read_the_same_postings_and_support_individual_block_fetches() {
    let raw = build(false);
    let compressed = build(true);
    assert!(compressed.len() * 4 < raw.len());

    let expected = SegmentReader::open(raw.clone()).unwrap();
    let actual = SegmentReader::open(compressed.clone()).unwrap();
    assert_eq!(actual.iter().unwrap(), expected.iter().unwrap());
    for (bytes, version) in [(raw, FORMAT_VERSION), (compressed, BLOCK_CODEC_VERSION)] {
        let metadata = SegmentReader::open_from_tail(&bytes, bytes.len() as u64).unwrap();
        assert_eq!(metadata.footer.format_version, version);
        for key in [b"id-000".as_slice(), b"id-107", b"id-199", b"z-raw"] {
            let offset = metadata.block_offset_for_key(key).unwrap();
            let (start, end) = metadata.block_byte_range(offset).unwrap();
            let block = metadata
                .decode_data_block(&bytes[start as usize..end as usize])
                .unwrap();
            let reader = BlockReader::open(&block).unwrap();
            assert_eq!(
                reader.get(key).unwrap(),
                expected.get(key).unwrap().as_deref()
            );
        }
    }
}

#[test]
fn a_bad_compressed_envelope_fails_as_corruption() {
    let original = build(true);
    assert_eq!(original[0], 1);
    for bytes in [
        [1, 0, 0, 0, 0],
        [1, 0, 0, 128, 0],
        [1, 8, 0, 0, 0],
        [255, 0, 0, 0, 0],
    ] {
        let mut invalid = original.clone();
        invalid[..5].copy_from_slice(&bytes);
        let reader = SegmentReader::open(invalid).unwrap();
        let error = reader.get(b"id-000").unwrap_err();
        assert!(matches!(error, ReaderError::Block(_)) && error.is_corruption());
    }
    let mut invalid = original;
    invalid[5] ^= 0xff;
    let error = SegmentReader::open(invalid)
        .unwrap()
        .get(b"id-000")
        .unwrap_err();
    assert!(error.is_corruption());
}

#[test]
fn tiny_blocks_stay_raw_in_the_versioned_envelope() {
    let mut writer = SegmentWriter::new_compressed(SegmentWriterOptions::default());
    writer.add(b"k", b"v").unwrap();
    let bytes = writer.finish().unwrap().data;
    assert_eq!(bytes[0], 0);
    let reader = SegmentReader::open(bytes).unwrap();
    assert_eq!(reader.get(b"k").unwrap().as_deref(), Some(b"v".as_slice()));
}
