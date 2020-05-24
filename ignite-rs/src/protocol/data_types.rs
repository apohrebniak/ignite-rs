use std::any::Any;
use std::convert::TryFrom;
use std::io::Read;

use crate::error::{IgniteError, IgniteResult};
use crate::protocol::*;
use crate::protocol::{read_u8, TypeCode};
use crate::{
    CollType, Collection, Enum, EnumArr, Map, MapType, ObjArr, PackType, Unpack, UnpackType,
};

/// Ignite's 'char' is a UTF-16 code UNIT, which means its size is 2 bytes.
/// As Rust's 'char' is a Unicode scalar value (a.k.a UTF-32 code unit) and has 4 bytes,
/// I don't see how the API should be properly implemented. u16 is used for now

macro_rules! pack_type {
    ($t:ty, $code:path, $pack_fn:ident) => {
        impl PackType for $t {
            fn pack(self) -> Vec<u8> {
                pack_data_obj($code, &mut $pack_fn(self))
            }
        }
    };
}

pack_type!(u8, TypeCode::Byte, pack_u8);
pack_type!(u16, TypeCode::Char, pack_u16);
pack_type!(i16, TypeCode::Short, pack_i16);
pack_type!(i32, TypeCode::Int, pack_i32);
pack_type!(i64, TypeCode::Long, pack_i64);
pack_type!(f32, TypeCode::Float, pack_f32);
pack_type!(f64, TypeCode::Double, pack_f64);
pack_type!(bool, TypeCode::Bool, pack_bool);
pack_type!(String, TypeCode::String, pack_string);
pack_type!(Uuid, TypeCode::Uuid, pack_uuid);
pack_type!(Enum, TypeCode::Enum, pack_enum);
pack_type!(Timestamp, TypeCode::Timestamp, pack_timestamp);
pack_type!(Date, TypeCode::Date, pack_date);
pack_type!(Time, TypeCode::Time, pack_time);
pack_type!(Decimal, TypeCode::Decimal, pack_decimal);

macro_rules! unpack_type {
    ($t:ty, $unpack_fn:ident) => {
        impl UnpackType for $t {
            fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
                let type_code = TypeCode::try_from(read_u8(reader)?)?;
                let value: Option<Self> = match type_code {
                    TypeCode::Null => None,
                    _ => Some($unpack_fn(reader)?),
                };
                Ok(value)
            }
        }
    };
}

unpack_type!(u8, read_u8);
unpack_type!(u16, read_u16);
unpack_type!(i16, read_i16);
unpack_type!(i32, read_i32);
unpack_type!(i64, read_i64);
unpack_type!(f32, read_f32);
unpack_type!(f64, read_f64);
unpack_type!(bool, read_bool);
unpack_type!(String, read_string);
unpack_type!(Uuid, read_uuid);
unpack_type!(Enum, read_enum);
unpack_type!(Timestamp, read_timestamp);
unpack_type!(Date, read_date);
unpack_type!(Time, read_time);
unpack_type!(Decimal, read_decimal);

macro_rules! pack_primitive_arr {
    ($t:ty, $code:path, $pack_fn:ident) => {
        impl PackType for Vec<$t> {
            fn pack(self) -> Vec<u8> {
                let mut payload: Vec<u8> =
                    Vec::with_capacity(self.len() * std::mem::size_of::<$t>());
                payload.append(&mut pack_i32(self.len() as i32)); // length of array
                for el in self {
                    payload.append(&mut $pack_fn(el));
                }
                pack_data_obj($code, &mut payload)
            }
        }
    };
}

pack_primitive_arr!(u8, TypeCode::ArrByte, pack_u8);
pack_primitive_arr!(i16, TypeCode::ArrShort, pack_i16);
pack_primitive_arr!(i32, TypeCode::ArrInt, pack_i32);
pack_primitive_arr!(i64, TypeCode::ArrLong, pack_i64);
pack_primitive_arr!(f32, TypeCode::ArrFloat, pack_f32);
pack_primitive_arr!(f64, TypeCode::ArrDouble, pack_f64);
pack_primitive_arr!(bool, TypeCode::ArrBool, pack_bool);
pack_primitive_arr!(u16, TypeCode::ArrChar, pack_u16);

macro_rules! unpack_primitive_arr {
    ($t:ty, $read_fn:ident) => {
        impl UnpackType for Vec<$t> {
            fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
                let type_code = TypeCode::try_from(read_u8(reader)?)?;
                let value: Option<Self> = match type_code {
                    TypeCode::Null => None,
                    _ => Some(read_primitive_arr(reader, $read_fn)?),
                };
                Ok(value)
            }
        }
    };
}

unpack_primitive_arr!(u8, read_u8);
unpack_primitive_arr!(u16, read_u16);
unpack_primitive_arr!(i16, read_i16);
unpack_primitive_arr!(i32, read_i32);
unpack_primitive_arr!(i64, read_i64);
unpack_primitive_arr!(f32, read_f32);
unpack_primitive_arr!(f64, read_f64);
unpack_primitive_arr!(bool, read_bool);

macro_rules! pack_standard_arr {
    ($t:ty, $code:path) => {
        impl PackType for Vec<Option<$t>> {
            fn pack(self) -> Vec<u8> {
                let mut data: Vec<u8> = Vec::new();
                data.append(&mut pack_i32(self.len() as i32)); // length of array
                for item in self {
                    match item {
                        None => data.push(TypeCode::Null as u8),
                        Some(value) => data.append(&mut value.pack()),
                    }
                }
                pack_data_obj($code, &mut data)
            }
        }
    };
}

pack_standard_arr!(String, TypeCode::ArrString);
pack_standard_arr!(Decimal, TypeCode::ArrDecimal);
pack_standard_arr!(Time, TypeCode::ArrTime);
pack_standard_arr!(Timestamp, TypeCode::ArrTimestamp);
pack_standard_arr!(Uuid, TypeCode::ArrUuid);
pack_standard_arr!(Date, TypeCode::ArrDate);

macro_rules! unpack_standard_arr {
    ($t:ty) => {
        impl UnpackType for Vec<Option<$t>> {
            fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
                let type_code = TypeCode::try_from(read_u8(reader)?)?;
                let value: Option<Self> = match type_code {
                    TypeCode::Null => None,
                    _ => {
                        let len = read_i32(reader)?;
                        let mut data: Vec<Option<$t>> = Vec::with_capacity(len as usize);
                        for _ in 0..len {
                            let item = <$t>::unpack(reader)?;
                            data.push(item);
                        }
                        Some(data)
                    }
                };
                Ok(value)
            }
        }
    };
}

unpack_standard_arr!(String);
unpack_standard_arr!(Decimal);
unpack_standard_arr!(Time);
unpack_standard_arr!(Timestamp);
unpack_standard_arr!(Uuid);
unpack_standard_arr!(Date);

impl<T: PackType + UnpackType> PackType for ObjArr<T> {
    fn pack(self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        data.append(&mut pack_i32(self.type_id)); // typeid
        data.append(&mut pack_i32(self.elements.len() as i32)); // length of array
        for item in self.elements {
            data.append(&mut item.pack())
        }
        pack_data_obj(TypeCode::ArrObj, &mut data)
    }
}

impl<T: PackType + UnpackType> UnpackType for ObjArr<T> {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        let type_code = TypeCode::try_from(read_u8(reader)?)?;
        let value: Option<Self> = match type_code {
            TypeCode::Null => None,
            _ => {
                let type_id = read_i32(reader)?;
                let len = read_i32(reader)?;
                let mut data: Vec<Option<T>> = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let item = T::unpack(reader)?;
                    data.push(item);
                }
                Some(ObjArr::new(type_id, data))
            }
        };
        Ok(value)
    }
}

impl PackType for EnumArr {
    fn pack(self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        data.append(&mut pack_i32(self.type_id));
        data.append(&mut pack_i32(self.elements.len() as i32));
        for el in self.elements {
            data.append(&mut el.pack());
        }
        pack_data_obj(TypeCode::ArrEnum, &mut data)
    }
}

impl UnpackType for EnumArr {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        let type_code = TypeCode::try_from(read_u8(reader)?)?;
        let value: Option<Self> = match type_code {
            TypeCode::Null => None,
            _ => {
                let type_id = read_i32(reader)?;
                let len = read_i32(reader)?;
                let mut data: Vec<Option<Enum>> = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let item = Enum::unpack(reader)?;
                    data.push(item);
                }
                Some(EnumArr::new(type_id, data))
            }
        };
        Ok(value)
    }
}

impl<T: PackType + UnpackType> PackType for Collection<T> {
    fn pack(self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        data.append(&mut pack_i32(self.elements.len() as i32)); // length of array
        data.append(&mut pack_i8(self.coll_type as i8)); // collection type
        for item in self.elements {
            match item {
                None => data.push(TypeCode::Null as u8),
                Some(value) => data.append(&mut value.pack()),
            }
        }
        pack_data_obj(TypeCode::Collection, &mut data)
    }
}

impl<T: PackType + UnpackType> UnpackType for Collection<T> {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        let type_code = TypeCode::try_from(read_u8(reader)?)?;
        let value: Option<Self> = match type_code {
            TypeCode::Null => None,
            _ => {
                let len = read_i32(reader)?;
                let coll_type = CollType::try_from(read_i8(reader)?)?;
                let mut data: Vec<Option<T>> = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let item = T::unpack(reader)?;
                    data.push(item);
                }
                Some(Collection::new(coll_type, data))
            }
        };
        Ok(value)
    }
}

impl<K: PackType + UnpackType, V: PackType + UnpackType> PackType for Map<K, V> {
    fn pack(self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        data.append(&mut pack_i32(self.elements.len() as i32)); // length of array
        data.append(&mut pack_i8(self.map_type as i8)); // collection type
        for pair in self.elements {
            data.append(&mut pair.0.pack());
            data.append(&mut pair.1.pack());
        }
        pack_data_obj(TypeCode::Map, &mut data)
    }
}

impl<K: PackType + UnpackType, V: PackType + UnpackType> UnpackType for Map<K, V> {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        let type_code = TypeCode::try_from(read_u8(reader)?)?;
        let value: Option<Self> = match type_code {
            TypeCode::Null => None,
            _ => {
                let len = read_i32(reader)?;
                let map_type = MapType::try_from(read_i8(reader)?)?;
                let mut data: Vec<(Option<K>, Option<V>)> = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let key = K::unpack(reader)?;
                    let value = V::unpack(reader)?;
                    data.push((key, value));
                }
                Some(Map::new(map_type, data))
            }
        };
        Ok(value)
    }
}

impl<T: PackType> PackType for Option<T> {
    fn pack(self) -> Vec<u8> {
        match self {
            None => vec![TypeCode::Null as u8],
            Some(inner) => inner.pack(),
        }
    }
}

impl<T: UnpackType> UnpackType for Option<T> {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        let inner_op = T::unpack(reader)?;
        match inner_op {
            None => Ok(None),
            Some(inner) => Ok(Some(Some(inner))),
        }
    }
}
