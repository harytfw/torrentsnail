use super::{Error, Result, Value};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::forward_to_deserialize_any;
use std::collections::{BTreeMap, HashSet};
pub struct Deserializer<'a> {
    value: Option<&'a Value>,
}

impl<'a> Deserializer<'a> {
    pub fn new(v: Option<&'a Value>) -> Self {
        Self { value: v }
    }

    fn build_ref_dict(&'a self, m: &'a BTreeMap<String, Value>) -> BTreeMap<&'a str, &'a Value> {
        m.iter().map(|(k, v)| (k.as_str(), v)).collect()
    }

    fn build_ref_list(&'a self, l: &'a [Value]) -> Vec<&'a Value> {
        l.iter().collect()
    }
}

impl<'a, 'de> de::Deserializer<'de> for &mut Deserializer<'a> {
    type Error = Error;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value.take() {
            Some(v) => match v {
                Value::String(s) => return visitor.visit_str(s),
                Value::Integer(i) => return visitor.visit_i64(*i),
                Value::Bytes(b) => return visitor.visit_bytes(b),
                Value::Dictionary(m) => visitor.visit_map(MapAccessor::new(self.build_ref_dict(m))),
                Value::List(l) => visitor.visit_seq(SeqAccessor::new(self.build_ref_list(l))),
            },
            None => visitor.visit_none(),
        }
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value.take().expect("should not reused") {
            Value::Dictionary(m) => {
                let mut helper = MapAccessor::new(self.build_ref_dict(m));
                helper.set_valid_fields(fields);
                visitor.visit_map(helper)
            }
            _ => Err(Error::Deserialize("value is not dictionary".into())),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value.is_some() {
            true => visitor.visit_some(self),
            false => visitor.visit_none(),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct seq tuple
        tuple_struct map enum identifier ignored_any
    }
}

struct SeqAccessor<'a> {
    list_rev: Vec<&'a Value>,
}

impl<'a> SeqAccessor<'a> {
    fn new(mut list: Vec<&'a Value>) -> Self {
        list.reverse();
        SeqAccessor { list_rev: list }
    }
}

impl<'a, 'de> SeqAccess<'de> for SeqAccessor<'a> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if self.list_rev.is_empty() {
            return Ok(None);
        }
        let v = self.list_rev.pop();
        seed.deserialize(&mut Deserializer::new(v)).map(Some)
    }
}

struct MapAccessor<'a> {
    m: BTreeMap<&'a str, &'a Value>,
    value: Option<&'a Value>,
    valid_fields: Option<HashSet<&'static str>>,
}

impl<'a> MapAccessor<'a> {
    pub fn new(m: BTreeMap<&'a str, &'a Value>) -> Self {
        Self {
            m,
            value: None,
            valid_fields: None,
        }
    }

    pub fn set_valid_fields(&mut self, fields: &[&'static str]) {
        self.valid_fields = Some(fields.iter().cloned().collect());
    }
}

impl<'a, 'de> MapAccess<'de> for MapAccessor<'a> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        if self.m.is_empty() {
            return Ok(None);
        }

        let key = self.m.keys().next().unwrap().to_string();
        let entry = self.m.remove_entry(key.as_str()).unwrap();

        self.value = match &self.valid_fields {
            Some(fields) if !fields.contains(key.as_str()) => None,
            _ => Some(entry.1),
        };

        let v = Value::String(key.to_string());
        seed.deserialize(&mut Deserializer::new(Some(&v))).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        let mut de = Deserializer::new(self.value.take());
        seed.deserialize(&mut de)
    }
}
