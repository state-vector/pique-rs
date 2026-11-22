//! Exercise the real S3 SDK protocol boundary without credentials or an external service.
#![cfg(feature = "s3")]

use pique::{
    ReaderError, RemoteSegmentReader, S3Backend, SegmentWriter, SegmentWriterOptions,
    StorageBackend, StorageError,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
enum Response {
    Object(Vec<u8>),
    Error(u16, &'static str),
    Timeout,
    Truncated,
}

struct Fixture {
    endpoint: String,
    response: Arc<Mutex<Response>>,
    requests: Arc<Mutex<Vec<String>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(response: Response) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let response = Arc::new(Mutex::new(response));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = response.clone();
        let observed = requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let response = state.lock().unwrap().clone();
                let observed = observed.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        let mut buffer = [0; 4096];
                        let n = socket.read(&mut buffer).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&buffer[..n]);
                    }
                    let request = String::from_utf8(request).unwrap();
                    let first_line = request.lines().next().unwrap();
                    observed.lock().unwrap().push(first_line.to_owned());
                    let head = first_line.starts_with("HEAD ");
                    let (status, body, length) = match response {
                        Response::Timeout => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            return;
                        }
                        Response::Truncated => (200, b"short".to_vec(), 100),
                        Response::Error(status, code) => {
                            let body = if head {
                                Vec::new()
                            } else {
                                format!(
                                    "<Error><Code>{code}</Code><Message>fixture {code}</Message>\
                                     <RequestId>read-error-fixture</RequestId></Error>"
                                )
                                .into_bytes()
                            };
                            let len = body.len();
                            (status, body, len)
                        }
                        Response::Object(data) => {
                            if head {
                                (200, Vec::new(), data.len())
                            } else if let Some(range) = request.lines().find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("range")
                                    .then(|| value.trim().strip_prefix("bytes=").unwrap())
                            }) {
                                let (start, end) = range.split_once('-').unwrap();
                                let (start, end) = if start.is_empty() {
                                    (data.len() - end.parse::<usize>().unwrap(), data.len())
                                } else {
                                    (
                                        start.parse::<usize>().unwrap(),
                                        end.parse::<usize>().unwrap() + 1,
                                    )
                                };
                                (206, data[start..end].to_vec(), end - start)
                            } else {
                                let len = data.len();
                                (200, data, len)
                            }
                        }
                    };
                    let header = format!(
                        "HTTP/1.1 {status} Fixture\r\ncontent-length: {length}\r\n\
                         content-type: application/xml\r\nx-amz-request-id: read-error-fixture\r\n\
                         connection: close\r\n\r\n"
                    );
                    socket.write_all(header.as_bytes()).await.unwrap();
                    if !head {
                        socket.write_all(&body).await.unwrap();
                    }
                });
            }
        });
        Self {
            endpoint,
            response,
            requests,
            server,
        }
    }

    fn backend(&self) -> S3Backend {
        let config = aws_sdk_s3::config::Builder::new()
            .endpoint_url(&self.endpoint)
            .force_path_style(true)
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "fixture",
                "fixture",
                None,
                None,
                "s3-read-errors",
            ))
            .behavior_version_latest()
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_millis(200))
                    .build(),
            )
            .build();
        S3Backend::new(aws_sdk_s3::Client::from_conf(config), "test".into())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn read(backend: &S3Backend, operation: &str) -> Result<(), StorageError> {
    match operation {
        "size" => backend.object_size("immutable.osi").await.map(|_| ()),
        "range" => backend.read_range("immutable.osi", 0..1).await.map(|_| ()),
        "tail" => backend.read_tail("immutable.osi", 1).await.map(|_| ()),
        "all" => backend.read_all("immutable.osi").await.map(|_| ()),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn confirmed_missing_is_typed_for_every_s3_read() {
    let fixture = Fixture::new(Response::Error(404, "NoSuchKey")).await;
    let backend = fixture.backend();
    let mut failures = Vec::new();
    for operation in ["size", "range", "tail", "all"] {
        let error = read(&backend, operation).await.unwrap_err();
        if !matches!(&error, StorageError::NotFound(detail) if detail.contains("immutable.osi")) {
            failures.push(format!("{operation}: {error:?}"));
        }
    }
    assert_eq!(fixture.requests.lock().unwrap().len(), 4, "no extra probes");
    assert!(
        failures.is_empty(),
        "confirmed missing lost its type: {failures:?}"
    );
}

#[tokio::test]
async fn denied_unavailable_timeout_and_truncated_reads_are_not_missing() {
    for response in [
        Response::Error(403, "AccessDenied"),
        Response::Error(503, "SlowDown"),
        Response::Timeout,
        Response::Truncated,
    ] {
        let fixture = Fixture::new(response.clone()).await;
        let backend = fixture.backend();
        for operation in ["size", "range", "tail", "all"] {
            // HEAD observes metadata only; truncation concerns the body reads.
            if matches!(response, Response::Truncated) && operation == "size" {
                continue;
            }
            let error = read(&backend, operation).await.unwrap_err();
            assert!(
                matches!(error, StorageError::S3(_)),
                "{operation}: {error:?}"
            );
            assert!(ReaderError::Storage(error).is_transient());
        }
    }
}

#[tokio::test]
async fn get_404_with_another_service_code_does_not_guess_missing() {
    let fixture = Fixture::new(Response::Error(404, "OtherServiceFailure")).await;
    let backend = fixture.backend();
    for operation in ["range", "tail", "all"] {
        let error = read(&backend, operation).await.unwrap_err();
        assert!(
            matches!(error, StorageError::S3(_)),
            "{operation}: {error:?}"
        );
    }
}

#[tokio::test]
async fn healthy_reads_and_cached_reader_deletion_preserve_the_actual_cause() {
    let mut writer = SegmentWriter::new(SegmentWriterOptions::default());
    writer.add(b"key", b"value").unwrap();
    let data = writer.finish().unwrap().data;
    let fixture = Fixture::new(Response::Object(data.clone())).await;
    let backend = fixture.backend();
    assert_eq!(
        backend.object_size("immutable.osi").await.unwrap(),
        data.len() as u64
    );
    assert_eq!(
        backend.read_range("immutable.osi", 0..1).await.unwrap(),
        data[..1]
    );
    assert_eq!(
        backend.read_tail("immutable.osi", 1).await.unwrap(),
        data[data.len() - 1..]
    );
    assert_eq!(backend.read_all("immutable.osi").await.unwrap(), data);
    let reader =
        RemoteSegmentReader::open(Box::new(fixture.backend()), "immutable.osi".into(), None)
            .await
            .unwrap();
    assert_eq!(reader.get(b"key").await.unwrap(), Some(b"value".to_vec()));
    *fixture.response.lock().unwrap() = Response::Error(404, "NoSuchKey");
    let error = reader.get(b"key").await.unwrap_err();
    assert!(
        matches!(&error, ReaderError::Storage(StorageError::NotFound(detail))
        if detail.contains("immutable.osi")),
        "{error:?}"
    );
    assert!(
        !error.is_transient(),
        "an immutable object is confirmed missing"
    );
    assert!(
        matches!(
            std::error::Error::source(&error).and_then(|s| s.downcast_ref::<StorageError>()),
            Some(StorageError::NotFound(_))
        ),
        "storage attribution must survive the reader"
    );
}

#[tokio::test]
async fn compressed_blocks_keep_the_same_point_read_request_count() {
    let value = b"s3://lodestar/tenants/org/repo=owner%2Frepo/shard.parquet".repeat(5);
    let options = SegmentWriterOptions {
        block_size: 4096,
        ..Default::default()
    };
    let mut observed = Vec::new();
    for mut writer in [
        SegmentWriter::new(options.clone()),
        SegmentWriter::new_compressed(options),
    ] {
        for index in 0..200 {
            writer
                .add(format!("key-{index:03}").as_bytes(), &value)
                .unwrap();
        }
        let data = writer.finish().unwrap().data;
        let size = data.len();
        let fixture = Fixture::new(Response::Object(data)).await;
        let reader =
            RemoteSegmentReader::open(Box::new(fixture.backend()), "immutable.osi".into(), None)
                .await
                .unwrap();
        for key in [b"key-000", b"key-100", b"key-199"] {
            assert_eq!(
                reader.get(key).await.unwrap().as_deref(),
                Some(value.as_slice())
            );
        }
        let requests = fixture.requests.lock().unwrap();
        let heads = requests
            .iter()
            .filter(|line| line.starts_with("HEAD "))
            .count();
        let gets = requests
            .iter()
            .filter(|line| line.starts_with("GET "))
            .count();
        assert_eq!((heads, gets), (1, 4), "one tail and one range per lookup");
        observed.push(size);
    }
    assert!(observed[1] * 4 < observed[0]);
}
