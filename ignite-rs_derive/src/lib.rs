use proc_macro2::{Ident, Span, TokenStream};
use quote::*;
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Fields, FieldsNamed, Meta};

#[proc_macro_derive(IgniteObj)]
pub fn derive_ignite_obj(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(item as DeriveInput);

    let type_name = &input.ident; // name of the struct
    let output = match input.data {
        Data::Struct(ref st) => match st.fields {
            Fields::Named(ref fields) => {
                let pack_tokens = impl_pack(type_name, fields);
                let unpack_tokens = impl_unpack(type_name, fields);

                quote! {
                    #pack_tokens
                    #unpack_tokens
                }
            }
            _ => quote_spanned! { st.fields.span() => compile_error!("Named struct expected!");},
        },
        _ => quote_spanned! { input.span() => compile_error!("Named struct expected!");},
    };

    proc_macro::TokenStream::from(output)
}

/// Implements PackType trait
fn impl_pack(type_name: &Ident, fields: &FieldsNamed) -> TokenStream {
    let type_id: i32 = get_type_id(type_name);
    let schema_id = get_schema_id(fields);

    let fields_schema = fields.named.iter().map(|f| {
        let field_name = &f.ident;
        quote_spanned! { field_name.span() =>
            schema.append(&mut ignite_rs::protocol::pack_i32(ignite_rs::utils::string_to_java_hashcode(stringify!(#field_name)))); // field id
            schema.append(&mut ignite_rs::protocol::pack_i32(ignite_rs::protocol::COMPLEX_OBJ_HEADER_LEN + fields.len() as i32)); // field offset
            fields.append(&mut self.#field_name.pack());
        }
    });

    quote! {
        impl PackType for #type_name {
            fn pack(&self) -> Vec<u8> {
                let mut data: Vec<u8> = Vec::new();
                data.append(&mut ignite_rs::protocol::pack_u8(1)); //version. always 1
                data.append(&mut ignite_rs::protocol::pack_u16(ignite_rs::protocol::FLAG_USER_TYPE|ignite_rs::protocol::FLAG_HAS_SCHEMA)); //flags
                data.append(&mut ignite_rs::protocol::pack_i32(#type_id)); //type_id

                //prepare buffers
                let mut fields: Vec<u8> = Vec::new();
                let mut schema: Vec<u8> = Vec::new();

                //pack fields
                #( #fields_schema)*

                data.append(&mut ignite_rs::protocol::pack_i32(ignite_rs::utils::bytes_to_java_hashcode(fields.as_slice()))); //hash_code. used for keys
                data.append(&mut ignite_rs::protocol::pack_i32(COMPLEX_OBJ_HEADER_LEN + fields.len() as i32 + schema.len() as i32)); //length. including header
                data.append(&mut ignite_rs::protocol::pack_i32(#schema_id)); //schema_id
                data.append(&mut ignite_rs::protocol::pack_i32(COMPLEX_OBJ_HEADER_LEN + fields.len() as i32)); //schema offset
                data.append(&mut fields); //object fields
                data.append(&mut schema); //schema
                // no raw_data_offset written
                ignite_rs::protocol::pack_data_obj(ignite_rs::protocol::TypeCode::ComplexObj, &mut data)
            }
        }
    }
}

/// Implements Unpack trait
fn impl_unpack(type_name: &Ident, fields: &FieldsNamed) -> TokenStream {
    let exp_type_id: i32 = get_type_id(type_name);
    let fields_count = fields.named.len();

    let fields_unpack = fields.named.iter().map(|f| {
        let field_name = &f.ident;
        let ty = &f.ty;
        let formatted_name = format_ident!("_{}", field_name.as_ref().unwrap().to_string());
        quote_spanned! { field_name.span() =>
            let #formatted_name = <#ty>::unpack(reader)?.unwrap(); // get option value
        }
    });

    let field_pairs = fields.named.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let formatted_name = format_ident!("_{}", field_name);
        quote! (#field_name: #formatted_name,)
    });

    quote! {
            impl UnpackType for #type_name {
            fn unpack_unwrapped(type_code: TypeCode, reader: &mut impl Read) -> IgniteResult<Option<Self>> {
                let value: Option<Self> = match type_code {
                    TypeCode::Null => None,
                    _ => {
                        read_u8(reader)?; // read version. skip

                        let flags = read_u16(reader)?; // read and parse flags
                        if (flags & FLAG_HAS_SCHEMA) == 0 {
                            return Err(IgniteError::from("Serialized object schema expected!"));
                        }
                        if (flags & FLAG_COMPACT_FOOTER) != 0 {
                            return Err(IgniteError::from("Compact footer is not supported!"));
                        }
                        if (flags & FLAG_OFFSET_ONE_BYTE) != 0 || (flags & FLAG_OFFSET_TWO_BYTES) != 0 {
                            return Err(IgniteError::from("Schema offset=4 is expected!"));
                        }

                        let type_id = read_i32(reader)?; // read and check type_id
                        if type_id != #exp_type_id {
                            return Err(IgniteError::from(
                                format!("Unknown type id! {} expected!", #exp_type_id).as_str(),
                            ));
                        }

                        read_i32(reader)?; // read hashcode
                        read_i32(reader)?; // read length (with header)
                        read_i32(reader)?; // read schema id
                        read_i32(reader)?; // read schema offset

                        #( #fields_unpack)*

                        // read schema
                        for _ in 0..#fields_count {
                            read_i64(reader)?; // read one field (id and offset)
                        }

                        Some(
                            #type_name{
                                #(#field_pairs)*
                            }
                        )
                    }
                };
                Ok(value)
            }
        }
    }
}

/// Schema ID based on field hashcodes
fn get_schema_id(fields: &FieldsNamed) -> i32 {
    fields
        .named
        .iter()
        .map(|field| field.ident.as_ref().unwrap()) // can unwrap because fields are named
        .map(|ident| ident.to_string())
        .map(|name| string_to_java_hashcode(&name))
        .fold(FNV1_OFFSET_BASIS, |acc, hash| {
            let mut res = acc;
            res ^= hash & 0xFF;
            res = res.overflowing_mul(FNV1_PRIME).0;
            res ^= (hash >> 8) & 0xFF;
            res = res.overflowing_mul(FNV1_PRIME).0;
            res ^= (hash >> 16) & 0xFF;
            res = res.overflowing_mul(FNV1_PRIME).0;
            res ^= (hash >> 24) & 0xFF;
            res = res.overflowing_mul(FNV1_PRIME).0;
            res
        })
}

/// Java-like hashcode of type's name
fn get_type_id(ident: &Ident) -> i32 {
    string_to_java_hashcode(&ident.to_string())
}

/// FNV1 hash offset basis
const FNV1_OFFSET_BASIS: i32 = 0x811C_9DC5_u32 as i32;
/// FNV1 hash prime
const FNV1_PRIME: i32 = 0x0100_0193;

/// Converts string into Java-like hash code
fn string_to_java_hashcode(value: &str) -> i32 {
    let mut hash: i32 = 0;
    for char in value.chars().into_iter() {
        hash = 31i32.overflowing_mul(hash).0 + char as i32;
    }
    hash
}
