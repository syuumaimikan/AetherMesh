//! AWS Signature Version 4.
//!
//! Small enough to write out, and writing it out is why the AWS adapter needs
//! no AWS SDK. The algorithm is public and stable: canonical request, string to
//! sign, a chain of HMACs for the key, hex signature in an `Authorization`
//! header.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::CloudError;

type HmacSha256 = Hmac<Sha256>;

/// Everything one signature needs.
pub struct Request<'a> {
    pub method: &'a str,
    pub host: &'a str,
    /// Path with an optional query string, e.g. `/?Action=DescribeInstances`.
    pub path: &'a str,
    pub body: &'a [u8],
    pub access_key_id: &'a str,
    pub secret_access_key: &'a str,
    pub session_token: Option<&'a str>,
    pub region: &'a str,
    pub service: &'a str,
    pub timestamp: std::time::SystemTime,
}

/// Signs a request and returns the headers to add to it.
pub fn sign(request: Request<'_>) -> Result<Vec<(String, String)>, CloudError> {
    let (date, datetime) = timestamps(request.timestamp);
    let payload_hash = hex::encode(Sha256::digest(request.body));

    let (path, query) = split_query(request.path);
    let canonical_query = canonical_query(query);

    // Signed headers, in the order the algorithm requires: lexicographic.
    let mut headers = vec![
        ("host".to_string(), request.host.to_string()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), datetime.clone()),
    ];
    if let Some(token) = request.session_token {
        headers.push(("x-amz-security-token".to_string(), token.to_string()));
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect::<String>();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method, path, canonical_query, canonical_headers, signed_headers, payload_hash
    );

    let scope = format!("{date}/{}/{}/aws4_request", request.region, request.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );

    let signature = hex::encode(
        signing_key(
            request.secret_access_key,
            &date,
            request.region,
            request.service,
        )?
        .chain_update(string_to_sign.as_bytes())
        .finalize()
        .into_bytes(),
    );

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        request.access_key_id
    );

    // `host` is set by the HTTP client itself, so it is signed but not resent.
    let mut result: Vec<(String, String)> = headers
        .into_iter()
        .filter(|(name, _)| name != "host")
        .collect();
    result.push(("authorization".to_string(), authorization));
    Ok(result)
}

/// `AWS4` + secret, then date, region, service, `aws4_request`.
fn signing_key(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
) -> Result<HmacSha256, CloudError> {
    let mut key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    key = hmac(&key, region.as_bytes())?;
    key = hmac(&key, service.as_bytes())?;
    key = hmac(&key, b"aws4_request")?;

    HmacSha256::new_from_slice(&key).map_err(|error| CloudError::Request(error.to_string()))
}

fn hmac(key: &[u8], data: &[u8]) -> Result<Vec<u8>, CloudError> {
    let mut mac =
        HmacSha256::new_from_slice(key).map_err(|error| CloudError::Request(error.to_string()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

/// `YYYYMMDD` and `YYYYMMDDTHHMMSSZ`, computed without a date library.
fn timestamps(now: std::time::SystemTime) -> (String, String) {
    let seconds = now
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);

    let days = seconds / 86_400;
    let time_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days as i64);

    (
        format!("{year:04}{month:02}{day:02}"),
        format!(
            "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
            time_of_day / 3600,
            (time_of_day % 3600) / 60,
            time_of_day % 60
        ),
    )
}

/// Days since the epoch to a calendar date (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;

    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn split_query(path: &str) -> (&str, &str) {
    match path.split_once('?') {
        Some((path, query)) => (path, query),
        None => (path, ""),
    }
}

/// Query parameters sorted by name, as the algorithm requires.
fn canonical_query(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }

    let mut pairs: Vec<(&str, &str)> = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .collect();
    pairs.sort();

    pairs
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    fn request<'a>(path: &'a str, body: &'a [u8]) -> Request<'a> {
        Request {
            method: "GET",
            host: "ec2.us-east-1.amazonaws.com",
            path,
            body,
            access_key_id: "AKIDEXAMPLE",
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            session_token: None,
            region: "us-east-1",
            service: "ec2",
            // 2015-08-30T12:36:00Z, the date from the AWS test suite.
            timestamp: UNIX_EPOCH + Duration::from_secs(1_440_938_160),
        }
    }

    #[test]
    fn the_timestamp_is_formatted_the_way_aws_wants_it() {
        let (date, datetime) = timestamps(UNIX_EPOCH + Duration::from_secs(1_440_938_160));

        assert_eq!(date, "20150830");
        assert_eq!(datetime, "20150830T123600Z");
    }

    #[test]
    fn dates_convert_across_leap_years() {
        let (date, _) = timestamps(UNIX_EPOCH + Duration::from_secs(1_582_934_400));
        assert_eq!(date, "20200229");

        let (date, _) = timestamps(UNIX_EPOCH);
        assert_eq!(date, "19700101");
    }

    #[test]
    fn query_parameters_are_sorted() {
        assert_eq!(canonical_query("b=2&a=1"), "a=1&b=2");
        assert_eq!(canonical_query(""), "");
        assert_eq!(canonical_query("Action=Describe"), "Action=Describe");
    }

    #[test]
    fn signing_produces_the_expected_header_shape() {
        let headers = sign(request(
            "/?Action=DescribeInstances&Version=2016-11-15",
            b"",
        ))
        .unwrap();

        let authorization = headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .map(|(_, value)| value.clone())
            .expect("authorization header");

        assert!(authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/ec2/aws4_request"
        ));
        assert!(authorization.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        // 64 hex characters of signature.
        let signature = authorization.rsplit("Signature=").next().unwrap();
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn the_signature_depends_on_the_request() {
        let one = sign(request("/?Action=DescribeInstances", b"")).unwrap();
        let two = sign(request("/?Action=RunInstances", b"")).unwrap();

        assert_ne!(one, two);
    }

    #[test]
    fn a_session_token_is_signed_and_sent() {
        let mut base = request("/", b"");
        base.session_token = Some("TOKEN");
        let headers = sign(base).unwrap();

        assert!(
            headers
                .iter()
                .any(|(name, value)| name == "x-amz-security-token" && value == "TOKEN")
        );
        let authorization = headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .unwrap();
        assert!(authorization.1.contains("x-amz-security-token"));
    }

    #[test]
    fn signing_is_reproducible() {
        assert_eq!(
            sign(request("/", b"body")).unwrap(),
            sign(request("/", b"body")).unwrap()
        );
    }
}
