#![forbid(unsafe_code)]

//! The gRPC logic technology — a technology of `xmip-core-logic`.
//!
//! The path `/package.Service/Method` names the operation; the body is a
//! length-prefixed message — one byte compressed flag, four bytes big-endian
//! length, the protobuf bytes — and that message is the arguments. A result
//! goes back as one frame with `grpc-status: 0` in the trailers; a fault as a
//! non-zero `grpc-status` and a `grpc-message`, in the trailers too.
//! Metadata travels as headers both ways. Unary calls only: a stream of
//! messages is a stream of invocations, and that is the transport's to
//! deliver one at a time.
//!
//! gRPC rides HTTP/2, which `net::http2` speaks and the `http` transport
//! agrees per connection (ADR-0043, amended 2026-09-25): the message on a
//! stream of its own, `content-type: application/grpc`, the status in the
//! trailing header block. This technology frames and names; the
//! connection stays the transport's, and the tests call a method across a
//! loopback HTTP/2 connection to show the two fit.

use contract::ContractId;
use logic::{
    Arrival, Fault, Header, Invocation, Logic, LogicError, OperationName, Outcome, Reply, Request,
};
use stream::Stream;

const CONTENT_TYPE: &str = "application/grpc";
const ARGUMENTS: &str = "application/protobuf";

/// The gRPC technology.
pub struct Grpc;

/// The first message in a length-prefixed body, and whether it was flagged
/// compressed.
fn unframe(body: &[u8]) -> Result<(&[u8], bool), LogicError> {
    if body.len() < 5 {
        return Err(LogicError::new(
            "the body is shorter than a gRPC frame prefix",
        ));
    }
    let compressed = body[0] != 0;
    let length = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    body.get(5..5 + length)
        .map(|message| (message, compressed))
        .ok_or_else(|| {
            LogicError::new(format!(
                "the frame claims {length} bytes, the body has fewer"
            ))
        })
}

fn frame(message: &[u8]) -> Result<Vec<u8>, LogicError> {
    let length = u32::try_from(message.len())
        .map_err(|_| LogicError::new("a message over 4 GiB does not fit a frame"))?;
    let mut out = Vec::with_capacity(5 + message.len());
    out.push(0);
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(message);
    Ok(out)
}

/// `/package.Service/Method` into its two names.
fn name_of(target: &str) -> Result<OperationName, LogicError> {
    let path = target
        .split('?')
        .next()
        .unwrap_or(target)
        .trim_start_matches('/');
    match path.split_once('/') {
        Some((service, method))
            if !service.is_empty() && !method.is_empty() && !method.contains('/') =>
        {
            Ok(OperationName::new(service, method))
        }
        _ => Err(LogicError::new(format!(
            "{target:?} is not /package.Service/Method"
        ))),
    }
}

fn metadata(headers: &[Header]) -> Vec<Header> {
    headers
        .iter()
        .filter(|h| {
            let name = h.name.to_ascii_lowercase();
            !name.starts_with(':')
                && name != "content-type"
                && name != "te"
                && name != "grpc-status"
                && name != "grpc-message"
        })
        .cloned()
        .collect()
}

fn status_code(fault: &Fault) -> &str {
    match fault.code.as_str() {
        "CANCELLED" => "1",
        "UNKNOWN" => "2",
        "INVALID_ARGUMENT" => "3",
        "DEADLINE_EXCEEDED" => "4",
        "NOT_FOUND" => "5",
        "ALREADY_EXISTS" => "6",
        "PERMISSION_DENIED" => "7",
        "RESOURCE_EXHAUSTED" => "8",
        "FAILED_PRECONDITION" => "9",
        "ABORTED" => "10",
        "OUT_OF_RANGE" => "11",
        "UNIMPLEMENTED" => "12",
        "INTERNAL" => "13",
        "UNAVAILABLE" => "14",
        "DATA_LOSS" => "15",
        "UNAUTHENTICATED" => "16",
        numeric if numeric.parse::<u8>().is_ok() => numeric,
        _ => "2",
    }
}

impl Logic for Grpc {
    fn technology(&self) -> &'static str {
        "grpc"
    }

    fn invocation(&self, arrival: &Arrival<'_>) -> Result<Invocation, LogicError> {
        let operation = name_of(arrival.target)?;
        let (message, compressed) = unframe(arrival.body.bytes())?;
        let mut parameters = metadata(arrival.headers);
        if compressed {
            // The bytes are passed through with the fact recorded; decoding is
            // the message representation's concern, not the method's.
            parameters.push(Header::new("grpc-compressed", "true"));
        }
        Ok(Invocation {
            operation,
            arguments: Stream::new(
                arrival.body.id(),
                message.to_vec(),
                Some(ARGUMENTS.to_string()),
            ),
            parameters,
            contract: Some(ContractId("protobuf".to_string())),
        })
    }

    fn reply(&self, invocation: &Invocation, outcome: &Outcome) -> Result<Reply, LogicError> {
        let id = invocation.arguments.id();
        let headers = vec![
            Header::new(":status", "200"),
            Header::new("content-type", CONTENT_TYPE),
        ];
        Ok(match outcome {
            Outcome::Result(result) => Reply {
                headers,
                body: Stream::new(id, frame(result.bytes())?, Some(CONTENT_TYPE.to_string())),
                trailers: vec![Header::new("grpc-status", "0")],
            },
            Outcome::Fault(fault) => Reply {
                headers,
                body: Stream::new(id, Vec::new(), Some(CONTENT_TYPE.to_string())),
                trailers: vec![
                    Header::new("grpc-status", status_code(fault)),
                    Header::new("grpc-message", fault.message.clone()),
                ],
            },
        })
    }

    fn request(&self, invocation: &Invocation) -> Result<Request, LogicError> {
        let mut headers = vec![
            Header::new("content-type", CONTENT_TYPE),
            Header::new("te", "trailers"),
        ];
        headers.extend(metadata(&invocation.parameters));
        Ok(Request {
            target: format!(
                "/{}/{}",
                invocation.operation.service, invocation.operation.name
            ),
            method: "POST".to_string(),
            headers,
            body: Stream::new(
                invocation.arguments.id(),
                frame(invocation.arguments.bytes())?,
                Some(CONTENT_TYPE.to_string()),
            ),
        })
    }

    fn outcome(&self, invocation: &Invocation, reply: &Reply) -> Result<Outcome, LogicError> {
        // The trailers first; a reply with no body may carry its status in
        // its only header block, which gRPC calls Trailers-Only.
        let field = |name: &str| {
            reply
                .trailers
                .iter()
                .chain(&reply.headers)
                .find(|h| h.name.eq_ignore_ascii_case(name))
                .map(|h| h.value.as_str())
        };
        let status = field("grpc-status").unwrap_or("0");
        if status != "0" {
            return Ok(Outcome::Fault(Fault {
                code: status.to_string(),
                message: field("grpc-message").unwrap_or_default().to_string(),
            }));
        }
        let (message, _) = unframe(reply.body.bytes())?;
        Ok(Outcome::Result(Stream::new(
            invocation.arguments.id(),
            message.to_vec(),
            Some(ARGUMENTS.to_string()),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xcore::StreamId;

    fn stream(bytes: &[u8]) -> Stream {
        Stream::new(
            StreamId::new(1),
            bytes.to_vec(),
            Some(CONTENT_TYPE.to_string()),
        )
    }

    fn headers(fields: &[(String, String)]) -> Vec<Header> {
        fields
            .iter()
            .map(|(name, value)| Header::new(name.clone(), value.clone()))
            .collect()
    }

    /// The Receive side, as a transport plays it: the request off an
    /// HTTP/2 stream as an arrival, `GetOrder` answered with its argument
    /// reversed and every other method unimplemented, and the reply as
    /// the answer on the same stream.
    fn served(request: &net::http::Request) -> net::http::Response {
        assert_eq!(request.header_value("content-type"), Some(CONTENT_TYPE));
        assert_eq!(request.header_value("te"), Some("trailers"));
        let fields = headers(&request.headers);
        let body = stream(&request.body);
        let arrival = Arrival {
            target: &request.path,
            method: &request.method,
            headers: &fields,
            body: &body,
        };
        let invocation = Grpc.invocation(&arrival).expect("an invocation");
        let outcome = if invocation.operation.name == "GetOrder" {
            let mut bytes = invocation.arguments.bytes().to_vec();
            bytes.reverse();
            Outcome::Result(stream(&bytes))
        } else {
            Outcome::Fault(Fault {
                code: "UNIMPLEMENTED".into(),
                message: "no such method".into(),
            })
        };
        let reply = Grpc.reply(&invocation, &outcome).expect("a reply");
        let status = reply
            .headers
            .iter()
            .find(|h| h.name == ":status")
            .map_or(200, |h| h.value.parse().expect("a status"));
        let mut answer = net::http::Response::new(status).body(reply.body.bytes());
        for header in reply.headers.iter().filter(|h| !h.name.starts_with(':')) {
            answer = answer.header(&header.name, &header.value);
        }
        for trailer in &reply.trailers {
            answer = answer.trailer(&trailer.name, &trailer.value);
        }
        answer
    }

    /// The Send side: `method` invoked, its request sent on a stream of
    /// the connection, and the answer read back as its outcome.
    fn call(
        client: &mut net::http2::Client<std::net::TcpStream>,
        authority: &str,
        method: &str,
    ) -> (net::http::Response, Outcome) {
        let invocation = Invocation {
            operation: OperationName::new("orders.OrderService", method),
            arguments: stream(b"\x08\x2a"),
            parameters: vec![Header::new("x-request-id", "7")],
            contract: None,
        };
        let request = Grpc.request(&invocation).expect("a request");
        let mut sent = net::http::Request::new(&request.method, request.target.clone())
            .header("Host", authority)
            .body(request.body.bytes());
        for header in &request.headers {
            sent = sent.header(&header.name, &header.value);
        }
        let answer = client.send(&sent).expect("answered");
        let reply = Reply {
            headers: headers(&answer.headers),
            body: stream(&answer.body),
            trailers: headers(&answer.trailers),
        };
        let outcome = Grpc.outcome(&invocation, &reply).expect("an outcome");
        (answer, outcome)
    }

    #[test]
    fn a_method_is_called_across_a_loopback_http_2_connection() {
        let wait = Some(std::time::Duration::from_secs(10));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let authority = listener.local_addr().expect("address").to_string();
        let far = std::thread::spawn(move || {
            let (socket, _) = listener.accept().expect("accept");
            socket.set_read_timeout(wait).expect("timeout");
            net::http2::serve(socket, served).expect("served")
        });
        let socket = std::net::TcpStream::connect(&authority).expect("connect");
        socket.set_read_timeout(wait).expect("timeout");
        let mut client = net::http2::Client::handshake(socket, "http").expect("handshake");

        let (answer, outcome) = call(&mut client, &authority, "GetOrder");
        assert_eq!(answer.header_value("content-type"), Some(CONTENT_TYPE));
        assert_eq!(answer.trailer_value("grpc-status"), Some("0"));
        assert_eq!(answer.header_value("grpc-status"), None);
        assert_eq!(answer.body, [0, 0, 0, 0, 2, 0x2a, 0x08]);
        match outcome {
            Outcome::Result(result) => assert_eq!(result.bytes(), b"\x2a\x08"),
            Outcome::Fault(fault) => panic!("unexpected {fault:?}"),
        }

        let (answer, outcome) = call(&mut client, &authority, "CancelOrder");
        assert_eq!(answer.trailer_value("grpc-status"), Some("12"));
        match outcome {
            Outcome::Fault(fault) => assert_eq!(
                (fault.code.as_str(), fault.message.as_str()),
                ("12", "no such method")
            ),
            Outcome::Result(_) => panic!("expected UNIMPLEMENTED"),
        }
        client.close();
        assert_eq!(far.join().expect("thread"), 2);
    }

    #[test]
    fn a_path_and_a_frame_become_the_operation_and_its_message() {
        let body = stream(&frame(b"\x0a\x01X").expect("frame"));
        let headers = vec![
            Header::new("x-request-id", "7"),
            Header::new("te", "trailers"),
        ];
        let arrival = Arrival {
            target: "/orders.OrderService/PlaceOrder",
            method: "POST",
            headers: &headers,
            body: &body,
        };
        let invocation = Grpc.invocation(&arrival).expect("invocation");
        assert_eq!(
            invocation.operation,
            OperationName::new("orders.OrderService", "PlaceOrder")
        );
        assert_eq!(invocation.arguments.bytes(), b"\x0a\x01X");
        assert_eq!(
            invocation.parameters,
            vec![Header::new("x-request-id", "7")]
        );
        let short = stream(b"\x00\x00");
        let arrival = Arrival {
            target: "/orders.OrderService/PlaceOrder",
            method: "POST",
            headers: &[],
            body: &short,
        };
        assert!(Grpc.invocation(&arrival).is_err());
        let bad = Arrival {
            target: "/orders",
            method: "POST",
            headers: &[],
            body: &body,
        };
        assert!(Grpc.invocation(&bad).is_err());
    }

    #[test]
    fn a_result_is_a_frame_with_status_zero_and_a_fault_is_a_status_with_a_message() {
        let invocation = Invocation {
            operation: OperationName::new("orders.OrderService", "PlaceOrder"),
            arguments: stream(b""),
            parameters: vec![],
            contract: None,
        };
        let ok = Grpc
            .reply(&invocation, &Outcome::Result(stream(b"\x10\x2a")))
            .expect("reply");
        assert_eq!(ok.trailers, [Header::new("grpc-status", "0")]);
        assert!(!ok.headers.iter().any(|h| h.name.starts_with("grpc-")));
        assert_eq!(ok.body.bytes(), b"\x00\x00\x00\x00\x02\x10\x2a");
        let fault = Fault {
            code: "NOT_FOUND".into(),
            message: "no such order".into(),
        };
        let refused = Grpc
            .reply(&invocation, &Outcome::Fault(fault))
            .expect("reply");
        assert!(refused.trailers.contains(&Header::new("grpc-status", "5")));
        assert!(refused.body.is_empty());
    }

    #[test]
    fn a_request_frames_and_an_outcome_reads_the_trailers() {
        let invocation = Invocation {
            operation: OperationName::new("orders.OrderService", "GetOrder"),
            arguments: stream(b"\x08\x2a"),
            parameters: vec![Header::new("authorization", "Bearer t")],
            contract: None,
        };
        let request = Grpc.request(&invocation).expect("request");
        assert_eq!(request.target, "/orders.OrderService/GetOrder");
        assert!(
            request
                .headers
                .contains(&Header::new("authorization", "Bearer t"))
        );
        assert_eq!(request.body.bytes()[..5], [0, 0, 0, 0, 2]);
        let answered = Reply {
            trailers: vec![Header::new("grpc-status", "0")],
            headers: vec![Header::new(":status", "200")],
            body: stream(&frame(b"\x10\x01").expect("frame")),
        };
        match Grpc.outcome(&invocation, &answered).expect("outcome") {
            Outcome::Result(result) => assert_eq!(result.bytes(), b"\x10\x01"),
            Outcome::Fault(fault) => panic!("unexpected {fault:?}"),
        }
        // Trailers-Only: no body, the status in the one header block.
        let refused = Reply {
            trailers: Vec::new(),
            headers: vec![
                Header::new("grpc-status", "5"),
                Header::new("grpc-message", "no such order"),
            ],
            body: stream(b""),
        };
        match Grpc.outcome(&invocation, &refused).expect("outcome") {
            Outcome::Fault(fault) => assert_eq!(
                fault,
                Fault {
                    code: "5".into(),
                    message: "no such order".into()
                }
            ),
            Outcome::Result(_) => panic!("expected a fault"),
        }
    }
}
