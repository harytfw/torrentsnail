use super::{Result, Value};
use byteorder::WriteBytesExt;
use std::io::Write;

pub struct Encoder<W> {
    w: W,
}

impl<'a, W: Write + 'a> Encoder<W> {
    pub fn new(output: W) -> Self {
        Self { w: output }
    }

    fn write_bytes(&mut self, b: &'a [u8]) -> Result<()> {
        self.w.write_all(b.len().to_string().as_bytes())?;
        self.w.write_u8(b':')?;
        self.w.write_all(b)?;
        Ok(())
    }

    pub fn inner_write(&mut self, v: &'a Value) -> Result<()> {
        match v {
            Value::Bytes(b) => self.write_bytes(b)?,
            Value::String(s) => self.write_bytes(s.as_bytes())?,
            Value::Integer(i) => {
                self.w.write_u8(b'i')?;
                self.w.write_all(i.to_string().as_bytes())?;
                self.w.write_u8(b'e')?;
            }
            Value::List(list) => {
                self.w.write_u8(b'l')?;
                for vv in list {
                    self.inner_write(vv)?;
                }
                self.w.write_u8(b'e')?;
            }
            Value::Dictionary(m) => {
                self.w.write_u8(b'd')?;
                for (k, v) in m {
                    self.write_bytes(k.as_bytes())?;
                    self.inner_write(v)?;
                }
                self.w.write_u8(b'e')?;
            }
        }
        Ok(())
    }

    pub fn encode(&mut self, v: &'a Value) -> Result<()> {
        self.inner_write(v)
    }
}
