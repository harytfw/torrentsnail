use super::{Error, Result, Value};
use std::{
    collections::BTreeMap,
    io::{Read},
    slice,
};

pub struct Decoder<R> {
    input: R,
    byte: Option<u8>,
    value: Option<Value>,
}

impl<R> Decoder<R> {
    pub fn new(input: R) -> Self {
        Self {
            input,
            value: None,
            byte: None,
        }
    }
}

impl<'a> Decoder<&'a [u8]> {
    pub fn decode_partial(&mut self) -> Result<(Option<Value>, Vec<u8>)> {
        let val = self.consume_value()?;
        let mut remain = Vec::with_capacity(self.input.len() + 1);
        if let Some(b) = self.byte {
            remain.push(b);
        }
        remain.extend(self.input);
        self.input = &[];
        Ok((val, remain))
    }
}

impl<R> Decoder<R>
where
    R: Read,
{
    pub fn decode(&mut self) -> Result<Option<Value>> {
        self.consume_value()
    }

    fn consume_value(&mut self) -> Result<Option<Value>> {
        if let Some(v) = self.value.take() {
            return Ok(Some(v));
        }
        self.next_value()
    }

    fn next_value(&mut self) -> Result<Option<Value>> {
        self.value.take();
        if self.is_end() {
            return Ok(None);
        }
        let v = match self.peek_byte()? {
            b'i' => {
                let v = self.read_integer()?;
                Value::Integer(v)
            }
            b'l' => {
                let v = self.read_list()?;
                Value::List(v)
            }
            b'd' => {
                let v = self.read_dictionary()?;
                Value::Dictionary(v)
            }
            b'0'..=b'9' => self.read_bytes()?,
            b => {
                return Err(Error::Decode(format!(
                    "unrecognized data type: {}",
                    b as char
                )))
            }
        };
        Ok(Some(v))
    }

    fn read_dictionary(&mut self) -> Result<BTreeMap<String, Value>> {
        self.consume_dict_start()?;

        let mut m: BTreeMap<String, Value> = Default::default();
        loop {
            match self.peek_byte()? {
                b'e' => {
                    self.consume_end()?;
                    break;
                }
                _ => {
                    let key_value = self.read_bytes()?;
                    match key_value {
                        Value::Bytes(s) => {
                            let key = String::from_utf8_lossy(&s);
                            let value = self
                                .next_value()?
                                .ok_or_else(|| Error::Decode("missing value".into()))?;
                            m.insert(key.to_string(), value);
                        }
                        _ => return Err(Error::Decode("key is not utf8 string".into())),
                    };
                }
            }
        }
        Ok(m)
    }

    fn read_bytes(&mut self) -> Result<Value> {
        let len = self.read_length()?;

        let mut buf = vec![0u8; len];

        self.input.read_exact(&mut buf)?;

        Ok(Value::Bytes(buf))
    }

    fn read_list(&mut self) -> Result<Vec<Value>> {
        self.consume_list_start()?;
        let mut v = vec![];
        loop {
            match self.peek_byte()? {
                b'e' => {
                    self.consume_end()?;
                    break;
                }
                _ => {
                    let elem = self
                        .next_value()?
                        .ok_or_else(|| Error::Decode("missing element".into()))?;
                    v.push(elem);
                }
            }
        }
        Ok(v)
    }

    fn read_integer(&mut self) -> Result<i64> {
        self.consume_integer_start()?;

        let mut buf = vec![];
        let mut b = 0u8;
        loop {
            self.input.read_exact(slice::from_mut(&mut b))?;
            if b == b'e' {
                break;
            }
            buf.push(b);
        }

        let s = String::from_utf8_lossy(&buf);
        let num = s
            .parse::<i64>()
            .map_err(|e| Error::Decode(format!("{e}")))?;
        Ok(num)
    }

    fn read_length(&mut self) -> Result<usize> {
        let mut buf = vec![];

        loop {
            match self.peek_byte()? {
                b':' => {
                    self.consume_sep()?;
                    break;
                }
                byte => {
                    self.consume(1)?;
                    buf.push(byte);
                }
            }
        }

        if buf.is_empty() {
            return Err(Error::Decode("length not found".to_string()));
        }

        let len = String::from_utf8_lossy(&buf)
            .parse::<usize>()
            .map_err(|e| Error::Decode(format!("{e}")))?;

        Ok(len)
    }

    fn consume_byte(&mut self, expected: u8) -> Result<()> {
        match self.peek_byte()? {
            actual if actual == expected => {
                self.consume(1)?;
                Ok(())
            }
            b => Err(Error::Decode(format!(
                "expect byte: {expected:?}, actually got: {b:?}"
            ))),
        }
    }

    fn peek_byte(&mut self) -> Result<u8> {
        if let Some(b) = self.byte {
            return Ok(b);
        }

        let mut b = 0u8;
        self.input.read_exact(slice::from_mut(&mut b))?;
        self.byte.replace(b);

        Ok(b)
    }

    fn consume(&mut self, mut len: usize) -> Result<()> {
        if len >= 1 && self.byte.is_some() {
            self.byte.take();
            len -= 1;
        }

        if len == 0 {
            return Ok(());
        }

        let mut b = vec![0; len];
        self.input.read_exact(&mut b)?;
        Ok(())
    }

    fn consume_dict_start(&mut self) -> Result<()> {
        self.consume_byte(b'd')
    }

    fn consume_list_start(&mut self) -> Result<()> {
        self.consume_byte(b'l')
    }

    fn consume_integer_start(&mut self) -> Result<()> {
        self.consume_byte(b'i')
    }

    fn consume_end(&mut self) -> Result<()> {
        self.consume_byte(b'e')
    }

    fn consume_sep(&mut self) -> Result<()> {
        self.consume_byte(b':')
    }

    pub fn is_end(&mut self) -> bool {
        self.peek_byte().is_err()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_partial_no_remain() {
        let s = b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re".as_slice();
        let (v, remain) = Decoder::new(s).decode_partial().unwrap();
        assert_eq!(remain.len(), 0);
        assert!(v.is_some());
    }

    #[test]
    fn decode_partial_with_remain() {
        let s = b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re12345".as_slice();
        let (v, remain) = Decoder::new(s).decode_partial().unwrap();
        assert_eq!(&remain, b"12345");
        assert!(v.is_some());

        let s = b"d8:msg_typei1e5:piecei0e10:total_sizei34256eexxxxxxxx".as_slice();
        let (v, remain) = Decoder::new(s).decode_partial().unwrap();
        assert_eq!(&remain, b"xxxxxxxx");
        assert!(v.is_some());
    }
}
