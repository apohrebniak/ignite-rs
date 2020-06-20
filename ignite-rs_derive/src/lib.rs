use proc_macro2::{Ident, TokenStream};
use quote::*;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, FieldsNamed};

#[proc_macro_derive(IgniteObj)]
pub fn derive_ignite_obj(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(item as DeriveInput);

    let type_name = &input.ident; // name of the struct
    let output = match input.data {
        Data::Struct(ref st) => match st.fields {
            Fields::Named(ref fields) => {
                let write_tokens = impl_write_type(type_name, fields);
                let read_tokens = impl_read_type(type_name, fields);

                quote! {
                    #write_tokens
                    #read_tokens
                }
            }
            _ => quote_spanned! { st.fields.span() => compile_error!("Named struct expected!");},
        },
        _ => quote_spanned! { input.span() => compile_error!("Named struct expected!");},
    };

    proc_macro::TokenStream::from(output)
}

/// Implements WritableType trait
fn impl_write_type(type_name: &Ident, fields: &FieldsNamed) -> TokenStream {
    let type_id: i32 = get_type_id(type_name);
    let schema_id = get_schema_id(fields);

    let fields_schema = fields.named.iter().map(|f| {
        let field_name = &f.ident;
        quote_spanned! { field_name.span() =>
            ignite_rs::protocol::write_i32(&mut schema, ignite_rs::utils::string_to_java_hashcode(stringify!(#field_name)))?; // field id
            ignite_rs::protocol::write_i32(&mut schema, ignite_rs::protocol::COMPLEX_OBJ_HEADER_LEN + fields.len() as i32)?; // field offset
            self.#field_name.write(&mut fields)?;
        }
    });

    let fields_schema_size = fields.named.iter().map(|f| {
        let field_name = &f.ident;
        quote_spanned! { field_name.span() =>
            size += self.#field_name.size() + 4 + 4; // field's size, field id, fields offset
        }
    });

    quote! {
        impl WritableType for #type_name {
            fn write(&self, writer: &mut dyn Write) -> std::io::Result<()> {
                ignite_rs::protocol::write_u8(writer, ignite_rs::protocol::TypeCode::ComplexObj as u8)?;
                ignite_rs::protocol::write_u8(writer,1)?; //version. always 1
                ignite_rs::protocol::write_u16(writer, ignite_rs::protocol::FLAG_USER_TYPE|ignite_rs::protocol::FLAG_HAS_SCHEMA)?; //flags
                ignite_rs::protocol::write_i32(writer, #type_id)?; //type_id

                //prepare buffers
                let mut fields: Vec<u8> = Vec::new();
                let mut schema: Vec<u8> = Vec::new();

                //write fields
                #( #fields_schema)*

                ignite_rs::protocol::write_i32(writer, ignite_rs::utils::bytes_to_java_hashcode(fields.as_slice()))?; //hash_code. used for keys
                ignite_rs::protocol::write_i32(writer, COMPLEX_OBJ_HEADER_LEN + fields.len() as i32 + schema.len() as i32)?; //length. including header
                ignite_rs::protocol::write_i32(writer, #schema_id)?; //schema_id
                ignite_rs::protocol::write_i32(writer, COMPLEX_OBJ_HEADER_LEN + fields.len() as i32)?; //schema offset
                writer.write_all(&fields)?; //object fields
                writer.write_all(&schema)?; //schema
                // no raw_data_offset written
                Ok(())
            }

            fn size(&self) -> usize {
                let mut size = COMPLEX_OBJ_HEADER_LEN as usize;
                //write fields and schema sized
                #( #fields_schema_size)*
                size
            }
        }
    }
}

/// Implements Unwrite trait
fn impl_read_type(type_name: &Ident, fields: &FieldsNamed) -> TokenStream {
    let exp_type_id: i32 = get_type_id(type_name);
    let fields_count = fields.named.len();

    let fields_read = fields.named.iter().map(|f| {
        let field_name = &f.ident;
        let ty = &f.ty;
        let formatted_name = format_ident!("_{}", field_name.as_ref().unwrap().to_string());
        quote_spanned! { field_name.span() =>
            let #formatted_name = <#ty>::read(reader)?.unwrap(); // get option value
        }
    });

    let field_pairs = fields.named.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let formatted_name = format_ident!("_{}", field_name);
        quote! (#field_name: #formatted_name,)
    });

    quote! {
            impl ReadableType for #type_name {
            fn read_unwrapped(type_code: TypeCode, reader: &mut impl Read) -> IgniteResult<Option<Self>> {
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

                        #( #fields_read)*

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
