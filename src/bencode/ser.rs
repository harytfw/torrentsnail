use super::{Error, Result, Value};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

pub struct Serializer;

impl<'a> serde::Serializer for &'a mut Serializer {
    type Ok = Value;

    type Error = Error;

    type SerializeSeq = SeqSerializer<'a>;
    type SerializeTuple = SeqSerializer<'a>;
    type SerializeTupleStruct = SeqSerializer<'a>;
    type SerializeTupleVariant = SeqSerializer<'a>;
    type SerializeMap = MapSerializer<'a>;
    type SerializeStruct = MapSerializer<'a>;
    type SerializeStructVariant = MapSerializer<'a>;
    
    fn is_human_readable(&self) -> bool {
        false
    }

    fn serialize_bool(self, v: bool) -> Result<Value> {
        self.serialize_i64(v as i64)
    }

    fn serialize_i8(self, v: i8) -> Result<Value> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Value> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Value> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Value> {
        Ok(Value::Integer(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Value> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Value> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Value> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<Value> {
        self.serialize_i64(v as i64)
    }

    fn serialize_f32(self, v: f32) -> Result<Value> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Value> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_char(self, v: char) -> Result<Value> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<Value> {
        Ok(Value::String(v.to_string()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Value> {
        Ok(Value::Bytes(v.to_vec()))
    }

    fn serialize_none(self) -> Result<Value> {
        Err(Error::Serialize("not supported none".into()))
    }

    fn serialize_some<T>(self, value: &T) -> Result<Value>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Value> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Value> {
        self.serialize_none()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<Value> {
        self.serialize_i64(variant_index as i64)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<Value>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Value>
    where
        T: ?Sized + Serialize,
    {
        let mut m = HashMap::with_capacity(1);
        m.insert(_variant, _value);
        let buf = m.serialize(self)?;
        Ok(buf)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(SeqSerializer::new(self, len))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(SeqSerializer::new(self, Some(_len)))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(MapSerializer::new(self, len))
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(MapSerializer::new(self, Some(_len)))
    }
}

pub struct SeqSerializer<'a> {
    s: &'a mut Serializer,
    list: Vec<Value>,
    // a: Vec<(&'a K, &'a )>
}

impl<'a> SeqSerializer<'a> {
    fn new(s: &'a mut Serializer, len: Option<usize>) -> Self {
        Self {
            s,
            list: Vec::with_capacity(len.unwrap_or(0)),
        }
    }
}

macro_rules! impl_seq {
    ($tra:ty) => {
        impl<'a> $tra for SeqSerializer<'a> {
            type Ok = Value;
            type Error = Error;

            fn serialize_element<T>(&mut self, value: &T) -> Result<()>
            where
                T: ?Sized + Serialize,
            {
                self.list.push(value.serialize(&mut *self.s)?);
                Ok(())
            }

            fn end(self) -> Result<Value> {
                Ok(Value::List(self.list))
            }
        }
    };

    ($($tra:ty),+ $(,)?) => {
        $(
            impl_seq!($tra);
        )+
    }
}

macro_rules! impl_tuple {

    ($tra:ty) => {
        impl<'a> $tra for SeqSerializer<'a> {
            type Ok = Value;
            type Error = Error;

            fn serialize_field<T>(&mut self, value: &T) -> Result<()>
            where
                T: ?Sized + Serialize,
            {
                self.list.push(value.serialize(&mut *self.s)?);
                Ok(())
            }

            fn end(self) -> Result<Value> {
                Ok(Value::List(self.list))
            }
        }
    };

    ($($tra:ty),+ $(,)?) => {
        $(
            impl_tuple!($tra);
        )+
    }
}

impl_seq!(serde::ser::SerializeSeq, serde::ser::SerializeTuple,);
impl_tuple!(
    serde::ser::SerializeTupleStruct,
    serde::ser::SerializeTupleVariant,
);

pub struct MapSerializer<'a> {
    s: &'a mut Serializer,
    map: BTreeMap<String, Value>,
    key: Option<String>,
}

impl<'a> MapSerializer<'a> {
    fn new(s: &'a mut Serializer, _len: Option<usize>) -> Self {
        Self {
            s,
            map: BTreeMap::new(),
            key: None,
        }
    }
}

impl<'a> serde::ser::SerializeMap for MapSerializer<'a> {
    type Ok = Value;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        match key.serialize(&mut *self.s)? {
            Value::String(s) => self.key = Some(s),
            _ => return Err(Error::Serialize("key must be string".into())),
        };
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let key = self.key.take().expect("missing key");
        let value = value.serialize(&mut *self.s)?;
        self.map.insert(key, value);
        Ok(())
    }

    fn end(self) -> Result<Value> {
        Ok(Value::Dictionary(self.map))
    }
}

impl<'a> serde::ser::SerializeStruct for MapSerializer<'a> {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(&mut *self.s)?;
        self.map.insert(key.to_string(), value);
        Ok(())
    }

    fn end(self) -> Result<Value> {
        Ok(Value::Dictionary(self.map))
    }
}

impl<'a> serde::ser::SerializeStructVariant for MapSerializer<'a> {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(&mut *self.s)?;
        self.map.insert(key.to_string(), value);
        Ok(())
    }

    fn end(self) -> Result<Value> {
        Ok(Value::Dictionary(self.map))
    }
}
