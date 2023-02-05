mod de;
mod decoder;
mod encoder;
mod ser;
mod value;
use crate::{Error, Result};
use de::Deserializer;
pub use decoder::Decoder;
pub use encoder::Encoder;
use ser::Serializer;
use serde::{Deserialize, Serialize};
use std::io::{self, Read};
pub use value::{Value, ValueVisitor};

pub fn from_reader<'a, T, R>(input: R) -> Result<T>
where
    T: Deserialize<'a>,
    R: Read + 'a,
{
    let value = Decoder::new(input).decode()?;
    let mut deserializer = Deserializer::new(value.as_ref());
    let t = T::deserialize(&mut deserializer)?;
    Ok(t)
}

pub fn from_bytes<'a, T, B>(input: &'a B) -> Result<T>
where
    T: Deserialize<'a>,
    B: AsRef<[u8]> + ?Sized,
{
    from_reader(input.as_ref())
}

pub fn from_bytes_with_remain<'a, T, B>(input: &'a B) -> Result<(T, Vec<u8>)>
where
    T: Deserialize<'a>,
    B: AsRef<[u8]> + ?Sized,
{
    let (value, remain) = Decoder::new(input.as_ref()).decode_partial()?;
    let mut deserializer = Deserializer::new(value.as_ref());
    let t = T::deserialize(&mut deserializer)?;
    Ok((t, remain))
}

pub fn from_str<'a, T>(input: &'a str) -> Result<T>
where
    T: Deserialize<'a>,
{
    from_bytes(input.as_bytes())
}

pub fn to_writer<T, W>(w: &mut W, value: &T) -> Result<()>
where
    T: ?Sized + Serialize,
    W: io::Write,
{
    let mut encoder = Encoder::new(w);
    encoder.encode(&value.serialize(&mut Serializer)?)?;
    Ok(())
}

pub fn to_bytes<T>(value: &T) -> Result<Vec<u8>>
where
    T: ?Sized + Serialize,
{
    let mut writer = vec![];
    to_writer(&mut writer, value)?;
    Ok(writer)
}

pub fn to_string<T>(value: &T) -> Result<String>
where
    T: ?Sized + Serialize,
{
    let buf = to_bytes(value)?;
    let s = String::from_utf8(buf).map_err(|e| Error::Serialize(e.to_string()))?;
    Ok(s)
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, HashMap};

    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn ser() {
        {
            to_bytes(&1).unwrap();
        }
        {
            to_bytes(&vec![1, 2, 3]).unwrap();
        }
        {
            let _list: Vec<Value> = vec![1.into(), "foo".into()];
            to_bytes(&vec![1, 2, 3]).unwrap();
        }
        {
            to_bytes(&Some(1)).unwrap();
        }
        {
            to_bytes::<Option<i64>>(&None).unwrap_err();
        }
        {
            let mut m = HashMap::new();
            m.insert("key", "value");
            to_bytes(&m).unwrap();
        }
        {
            #[derive(Serialize)]
            struct S {
                str: String,
                ref_str: &'static str,
                vec: Vec<i32>,
                num: i64,
                dict: BTreeMap<&'static str, &'static str>,
                val: Value,
                bytes: Value,
            }

            let s = S {
                str: "foo".into(),
                ref_str: "ref",
                vec: vec![1, 2, 3, 4, 0],
                num: 9999999,
                dict: [("foo", "bar"), ("hello", "world")].into_iter().collect(),
                val: Value::String("hello world".into()),
                bytes: Value::Bytes(vec![0x01, 0x02]),
            };

            to_bytes(&s).unwrap();
        }
    }

    #[derive(Debug, Clone, Deserialize, Serialize)]
    struct R {
        values: Value,
    }

    #[derive(Debug, Clone, Deserialize, Serialize)]
    struct A {
        r: Option<R>,
    }

    #[test]
    fn de() {
        let s = "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re";
        let _: A = from_str(s).unwrap();
        let _: Value = from_str(s).unwrap();
    }
}
