use super::Error;
use serde::{de::Visitor, Serialize};
use std::{collections::BTreeMap, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    String(String),
    Bytes(Vec<u8>),
    Integer(i64),
    List(Vec<Value>),
    Dictionary(BTreeMap<String, Value>),
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            Self::Bytes(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Self::Dictionary(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::String(s) => Some(s.as_bytes()),
            Self::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Self::List(list) => Some(list),
            _ => None,
        }
    }

    pub fn list_get(&self, index: usize) -> Option<&Value> {
        self.as_list().and_then(|list| list.get(index))
    }

    pub fn dict_get(&self, key: &str) -> Option<&Value> {
        self.as_dict().and_then(|dict| dict.get(key))
    }
}

impl std::ops::Index<&str> for Value {
    type Output = Value;
    fn index(&self, index: &str) -> &Self::Output {
        self.dict_get(index)
            .expect("value is not dictionary or key not found")
    }
}

impl std::ops::Index<usize> for Value {
    type Output = Value;
    fn index(&self, index: usize) -> &Self::Output {
        self.list_get(index)
            .expect("value is not list or index out of bound")
    }
}

impl<'a, T> From<&'a T> for Value
where
    T: Into<Value> + Clone + ?Sized,
{
    fn from(v: &'a T) -> Self {
        v.clone().into()
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        s.to_string().into()
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<&[u8]> for Value {
    fn from(b: &[u8]) -> Self {
        b.to_vec().into()
    }
}

impl From<Vec<Value>> for Value {
    fn from(b: Vec<Value>) -> Self {
        Self::List(b)
    }
}

impl From<&[Value]> for Value {
    fn from(b: &[Value]) -> Self {
        Self::List(b.to_vec())
    }
}

macro_rules! impl_number {
    ($t:ty) => {
        impl From<$t> for Value {
            fn from(n: $t) -> Self {
                Self::Integer(n as i64)
            }
        }
    };
    ($($t:ty),+ $(,)?) => {
        $(
            impl_number!($t);
        )+
    }
}

impl_number!(i8, i16, i32, i64, isize);
impl_number!(u8, u16, u32, u64, usize);
impl_number!(bool);

impl TryFrom<serde_json::Value> for Value {
    type Error = Error;

    fn try_from(j: serde_json::Value) -> std::result::Result<Self, Error> {
        let new_val = match j {
            serde_json::Value::Array(a) => {
                let mut t: Vec<Value> = Vec::with_capacity(a.len());
                for v in a {
                    t.push(v.try_into()?);
                }
                Self::List(t)
            }
            serde_json::Value::Object(o) => {
                let mut m: BTreeMap<String, Value> = BTreeMap::new();
                for (k, v) in o {
                    m.insert(k, v.try_into()?);
                }
                Self::Dictionary(m)
            }
            serde_json::Value::Number(o) => {
                let i = o.as_i64().ok_or_else(|| Error::TryFrom(o.to_string()))?;
                i.into()
            }
            serde_json::Value::Null => Err(Error::TryFrom("null".to_string()))?,
            serde_json::Value::Bool(b) => b.into(),
            serde_json::Value::String(s) => s.into(),
        };
        Ok(new_val)
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::String(s) => serializer.serialize_str(s),
            Value::Bytes(bytes) => serializer.serialize_bytes(bytes),
            Value::Integer(num) => serializer.serialize_i64(*num),
            Value::List(arr) => serializer.collect_seq(arr),
            Value::Dictionary(obj) => serializer.collect_map(obj),
        }
    }
}

impl<'de> serde::de::Deserialize<'de> for Value {
    fn deserialize<D>(d: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        d.deserialize_any(ValueVisitor)
    }
}

pub struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "Bencode value")
    }

    fn visit_string<E>(self, v: String) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::String(v))
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::String(v.to_string()))
    }

    fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Integer(v))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Bytes(v.to_vec()))
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut v = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(item) = seq.next_element()? {
            v.push(item);
        }
        Ok(Value::List(v))
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut m = BTreeMap::new();
        while let Some((k, v)) = map.next_entry()? {
            m.insert(k, v);
        }
        Ok(Value::Dictionary(m))
    }
}
