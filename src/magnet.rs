use crate::{Error, Result};
use url::Url;

fn get_single_value(uri: &Url, name: &str) -> Option<String> {
    get_multiple_values(uri, name).into_iter().next()
}

fn get_multiple_values(uri: &Url, name: &str) -> Vec<String> {
    let mut ret = vec![];
    for (k, v) in uri.query_pairs() {
        if name == k {
            ret.push(v.to_string())
        }
    }
    ret
}

pub struct MagnetURI {
    pub scheme: String,
    pub xt: String,
    pub dn: Option<String>,
    pub tr: Vec<String>,
}

impl MagnetURI {
    pub fn from_uri(uri: &str) -> Result<Self> {
        let uri = Url::parse(uri)?;
        let xt =
            get_single_value(&uri, "xt").ok_or_else(|| Error::Magnet("missing xt".to_string()))?;
        let dn = get_single_value(&uri, "dn");
        let tr = get_multiple_values(&uri, "tr");
        let magnet = Self {
            scheme: uri.scheme().to_owned(),
            xt,
            dn,
            tr,
        };
        Ok(magnet)
    }

    pub fn xt_protocol(&self) -> Option<&str> {
		let s = self.xt.as_str();

        let first = self.xt.find(|c| c == ':')? + 1;
		let s = &s[first..];

        let end = s.find(|c| c == ':')?;
        Some(&s[..end])
    }

    pub fn xt_hash(&self) -> Option<&str> {
		let s = self.xt.as_str();
  
        let pos = s.find(|c| c == ':')?;
		let s = &s[pos+1..];

        let pos = s.find(|c| c == ':')?;
        Some(&s[pos+1..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magnet_uri() {
        let magnet = MagnetURI::from_uri("magnet:?xt=urn:btih:c9e15763f722f23e98a29decdfae341b98d53056&dn=Cosmos%20Laundromat").unwrap();
        assert_eq!(magnet.scheme, "magnet");
        assert_eq!(magnet.xt, "urn:btih:c9e15763f722f23e98a29decdfae341b98d53056");
		assert_eq!(magnet.dn, Some("Cosmos Laundromat".to_string()));
        assert_eq!(magnet.xt_hash(), Some("c9e15763f722f23e98a29decdfae341b98d53056"));
        assert_eq!(magnet.xt_protocol(), Some("btih"));
		assert!(magnet.tr.is_empty());
    }
}
