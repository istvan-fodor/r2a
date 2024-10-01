use proc_macro2::TokenStream;
use quote::quote;


use anyhow::Result;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::env;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use syn::parse_str;
use syn::Ident;
use syn::ItemImpl;
use syn::ItemMod;
use syn::{visit::Visit, ItemStruct, Type};
use walkdir::WalkDir;

enum SourceCode {
    TokenStream(TokenStream),
    #[allow(dead_code)]
    String(String),
}

impl Display for SourceCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceCode::TokenStream(ts) => write!(f, "{}", ts),
            SourceCode::String(s) => write!(f, "{}", s),
        }
    }
}

struct StructVisitor<'a> {
    schema_name_format: String,
    structs_by_schema: &'a mut BTreeMap<String, ROSStruct>,
    structs_by_type: &'a mut BTreeMap<String, ROSStruct>,
    module_stack: Vec<String>,
    valid_structs: &'a HashSet<String>,
}

impl<'a> StructVisitor<'a> {
    fn current_module_path(&self) -> String {
        self.module_stack.join("::")
    }
}

#[derive(Debug, Clone)]
struct ROSStruct {
    packaged_name: String,
    schema_name: String,
    fields: Vec<ROSField>,
}

impl ROSStruct {
    pub fn new(packaged_name: String, schema_name: String) -> Self {
        ROSStruct {
            packaged_name,
            schema_name,
            fields: Vec::new(),
        }
    }

    pub fn add_field(&mut self, field: ROSField) {
        self.fields.push(field);
    }
}

#[derive(Debug, Clone)]
struct ROSField {
    name: String,
    native_type: String,
}

impl ROSField {
    pub fn new(name: String, native_type: String) -> Self {
        ROSField { name, native_type }
    }
}

struct TraitImplVisitor<'a> {
    desired_trait: &'a str,
    module_stack: Vec<String>,
    implementing_structs: &'a mut HashSet<String>,
}

impl<'a> TraitImplVisitor<'a> {
    fn current_module_path(&self) -> String {
        self.module_stack.join("::")
    }
}

impl<'a> Visit<'a> for TraitImplVisitor<'a> {
    fn visit_item_mod(&mut self, i: &'a ItemMod) {
        self.module_stack.push(i.ident.to_string()); 

        syn::visit::visit_item_mod(self, i);

        self.module_stack.pop(); 
    }

    fn visit_item_impl(&mut self, i: &'a ItemImpl) {
        if let Some((_, path, _)) = &i.trait_ {
            if is_desired_trait(path, self.desired_trait) {
                if let Type::Path(type_path) = &*i.self_ty {
                    if let Some(last_segment) = type_path.path.segments.last() {
                        let mut full_path = self.current_module_path();
                        if !full_path.is_empty() {
                            full_path.push_str("::");
                        }
                        full_path.push_str(&last_segment.ident.to_string());

                        self.implementing_structs.insert(full_path);
                    }
                }
            }
        }
        syn::visit::visit_item_impl(self, i);
    }
}
impl<'a> Visit<'a> for StructVisitor<'a> {
    fn visit_item_mod(&mut self, i: &'a ItemMod) {
        self.module_stack.push(i.ident.to_string()); 

        syn::visit::visit_item_mod(self, i);

        self.module_stack.pop(); 
    }

    fn visit_item_struct(&mut self, i: &'a ItemStruct) {

        println!("Found struct: {}", i.ident);

        let mut package_name = "".to_string();
        package_name.push_str(&self.current_module_path());
        package_name.push_str("::");
        package_name.push_str(&i.ident.to_string());
        if self.valid_structs.contains(&package_name) {
            let schema_name = self.schema_name_format.replace("{}", &i.ident.to_string());

            let mut my_struct = ROSStruct::new(package_name.clone(), schema_name.clone());

            for field in &i.fields {
                let field_type = type_to_string(&field.ty);
                my_struct.add_field(ROSField::new(
                    field.ident.as_ref().unwrap().to_string(),
                    field_type,
                ));
            }
            self.structs_by_schema
                .insert(schema_name, my_struct.clone());

            self.structs_by_type.insert(package_name, my_struct);
        }
        syn::visit::visit_item_struct(self, i);
    }
}

fn create_name(original_name: &str, suffix: &str) -> String {
    let name = format!("{}{}", original_name, suffix);
    name.replace("::", "_").replace('/', "_")
}

fn create_name_identity(original_name: &str, suffix: &str) -> Ident {
    Ident::new(
        create_name(original_name, suffix).as_str(),
        proc_macro2::Span::call_site(),
    )
}

fn is_desired_trait(path: &syn::Path, trait_name: &str) -> bool {
    path.segments
        .last()
        .map_or(false, |segment| segment.ident == trait_name)
}

fn type_to_string(ty: &Type) -> String {
    match ty {
        Type::Path(type_path) => {
            type_path
                .path
                .segments
                .iter()
                .map(|segment| {
                    let ident = segment.ident.to_string();
                    if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                        let generics = args
                            .args
                            .iter()
                            .map(|arg| {
                                match arg {
                                    syn::GenericArgument::Type(ty) => type_to_string(ty),
                                    // Handle other types of generic arguments...
                                    _ => "".to_string(),
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{}<{}>", ident, generics)
                    } else {
                        ident
                    }
                })
                .collect::<Vec<_>>()
                .join("::")
        }
        _ => format!("{:?}", "x"),
    }
}

fn rust_type_to_arrow_type_token_stream(
    typ: &str,
    field_name: &str,
    nullable: bool,
) -> TokenStream {
    if typ == "Vec<u8>" {
        quote!(Field::new(#field_name, DataType::LargeBinary, #nullable))
    } else if typ.starts_with("Vec") {
        let type_token = match typ {
            "Vec<bool>" => quote!(DataType::Boolean),
            "Vec<str>" | "Vec<std::string::String>" => quote!(DataType::Utf8),
            "Vec<char>" => quote!(DataType::Utf8),
            "Vec<i8>" => quote!(DataType::Int8),
            "Vec<i16>" => quote!(DataType::Int16),
            "Vec<i32>" => quote!(DataType::Int32),
            "Vec<i64>" => quote!(DataType::Int64),
            "Vec<i128>" => quote!(DataType::Int64),
            "Vec<isize>" => quote!(DataType::Int64),
            "Vec<u16>" => quote!(DataType::UInt16),
            "Vec<u32>" => quote!(DataType::UInt32),
            "Vec<u64>" => quote!(DataType::UInt64),
            "Vec<u128>" => quote!(DataType::UInt64),
            "Vec<usize>" => quote!(DataType::UInt64),
            "Vec<f32>" => quote!(DataType::Float32),
            "Vec<f64>" => quote!(DataType::Float64),
            typ => panic!("Unupported type: {}", typ), // I guess in this case we just can't build?
        };
        quote!(Field::new(#field_name, DataType::LargeList(Arc::new(Field::new("item", #type_token, #nullable))), #nullable))
    } else {
        let type_token = match typ {
            "bool" => quote!(DataType::Boolean),
            "str" | "std::string::String" | "char" => quote!(DataType::Utf8),
            "()" => quote!(DataType::Null),
            "i8" => quote!(DataType::Int8),
            "i16" => quote!(DataType::Int16),
            "i32" => quote!(DataType::Int32),
            "i64" => quote!(DataType::Int64),
            "i128" => quote!(DataType::Int64), // Not exactly sure how to support this, but I haven't seen any ROS messages with this length
            "isize" => quote!(DataType::Int64),
            "u8" => quote!(DataType::UInt8),
            "u16" => quote!(DataType::UInt16),
            "u32" => quote!(DataType::UInt32),
            "u64" => quote!(DataType::UInt64),
            "u128" | "usize" => quote!(DataType::UInt64), // Arrow doesn't have u128
            "f32" => quote!(DataType::Float32),
            "f64" => quote!(DataType::Float64),
            typ => panic!("Unupported type: {}", typ), // I guess in this case we just can't build?
        };
        quote!(Field::new(#field_name, #type_token, #nullable))
    }
}

fn generate_imports() -> TokenStream {
    quote! {
        //use crate::{ROSField, ROSStruct};
    }
}

fn generate_arrow_imports() -> TokenStream {
    quote! {
        use arrow_schema::{DataType, Field, Fields, Schema};
        use r2r::{WrappedTypesupport};
    }
}

fn generate_supported_schema_list(structs_by_schema: &BTreeMap<String, ROSStruct>) -> TokenStream {
    let schema_names = structs_by_schema
        .values()
        .map(|ros_struct| &ros_struct.schema_name);

    let gen_function = quote! {

        pub static SUPPORTED_SCHEMAS: &'static [&'static str] = &[#(#schema_names),*];
    };

    gen_function
}

fn generate_arrow_schema_fields(
    schema: &str,
    structs_by_schema: &BTreeMap<String, ROSStruct>,
    structs_by_type: &BTreeMap<String, ROSStruct>,
    parent_json_path: &str,
    parent_field: &str,
    flat: bool,
) -> Vec<TokenStream> {
    let ros_struct = structs_by_schema.get(schema).unwrap();
    let mut schema_token_streams: Vec<TokenStream> = vec![];
    for field in &ros_struct.fields {
        let field_name = if !parent_field.is_empty() {
            format!("{}_{}", parent_field, field.name.clone())
        } else {
            field.name.clone()
        };

        let json_path = if !parent_json_path.is_empty() {
            format!("{}.{}", parent_json_path, field.name.clone())
        } else {
            field.name.clone()
        };

        let mut typ: Vec<TokenStream> = match field.native_type.as_str() {
            "bool"
            | "str"
            | "char"
            | "()"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "isize"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "usize"
            | "f32"
            | "f64"
            | "std::string::String"
            | "Vec<bool>"
            | "Vec<str>"
            | "Vec<char>"
            | "Vec<()>"
            | "Vec<i8>"
            | "Vec<i16>"
            | "Vec<i32>"
            | "Vec<i64>"
            | "Vec<i128>"
            | "Vec<isize>"
            | "Vec<u8>"
            | "Vec<u16>"
            | "Vec<u32>"
            | "Vec<u64>"
            | "Vec<u128>"
            | "Vec<usize>"
            | "Vec<f32>"
            | "Vec<f64>"
            | "Vec<std::string::String>" => {
                vec![rust_type_to_arrow_type_token_stream(
                    &field.native_type,
                    &field_name,
                    true,
                )]
            }
            typ if !flat && !typ.starts_with("Vec") => {
                let typ = format!("r2r::{}", typ);
                let field_struct = structs_by_type.get(&typ).unwrap();

                let type_underscore_name =
                    create_name_identity(&field_struct.packaged_name, "_Schema");

                let nullable = true;
                vec![quote!(
                    Field::new(#field_name, DataType::Struct(Fields::from(#type_underscore_name())), #nullable)
                )]
            }
            typ if flat && !typ.starts_with("Vec") => {
                let typ = format!("r2r::{}", typ);
                println!("{}", typ);
                let field_struct = structs_by_type.get(&typ).unwrap();
                generate_arrow_schema_fields(
                    &field_struct.schema_name,
                    structs_by_schema,
                    structs_by_type,
                    &json_path,
                    &field_name,
                    flat,
                )
            }
            typ => {
                //This is the case of a vector of complex types. These can't be flattened out as of now.

                let typ = &typ[4..typ.len() - 1];
                let typ = format!("r2r::{}", typ);
                let field_struct = structs_by_type.get(&typ).unwrap();

                let suffix = if flat { "_FlatSchema" } else { "_Schema" };

                let type_underscore_name =
                    create_name_identity(&field_struct.packaged_name, suffix);

                let nullable = true;

                if flat {
                    vec![quote!(
                        Field::new(#field_name, DataType::LargeList(Arc::new(Field::new("item", DataType::Struct(Fields::from(#type_underscore_name(false))), #nullable))), #nullable)
                    )]
                } else {
                    vec![quote!(
                        Field::new(#field_name, DataType::LargeList(Arc::new(Field::new("item", DataType::Struct(Fields::from(#type_underscore_name())), #nullable))), #nullable)
                    )]
                }
            }
        };
        schema_token_streams.append(&mut typ);
    }
    schema_token_streams
}

fn generate_flat_arrow_schema(
    structs_by_schema: &BTreeMap<String, ROSStruct>,
    structs_by_type: &BTreeMap<String, ROSStruct>,
) -> TokenStream {
    let (schema_fn_call, schema_fn): (Vec<TokenStream>, Vec<TokenStream>) = structs_by_schema
        .values()
        .map(|ros_struct| {
            let schema_name = &ros_struct.schema_name;
            let type_underscore_name_schema = create_name_identity(&ros_struct.packaged_name, "_FlatSchema");
            let type_underscore_name_schema_struct = create_name_identity(&ros_struct.packaged_name, "_Schema");

            let fields = generate_arrow_schema_fields(
                schema_name,
                structs_by_schema,
                structs_by_type,
                "$",
                "",
                true,
            );

            let fn_call = quote!(
                #schema_name => #type_underscore_name_schema(true),
            );

            let schema_fn = quote!(
                #[allow(non_snake_case)]
                pub fn #type_underscore_name_schema(include_self_struct: bool) -> Vec<Field> {
                    let mut schema = vec![#(#fields),*];
                    if include_self_struct {
                        schema.push(Field::new_struct("message_struct", #type_underscore_name_schema_struct(), true))
                    }
                    schema
                }
            );

            (fn_call, schema_fn)
        })
        .unzip();

    let gen_function = quote! {

        #[allow(dead_code)]
        pub(crate) fn map_ros_schema_to_flat_arrow_fields(ros_schema : &str) -> Vec<Field> {
            match ros_schema {
                #(#schema_fn_call)*
                unknown => {    
                    log::warn!("Unknown schema {}, using binary parser.", unknown);
                    vec![Field::new("binary_data", DataType::LargeBinary, true)]
                }
            }
        }

        #(#schema_fn)*

    };

    gen_function
}

fn generate_arrow_schema(
    structs_by_schema: &BTreeMap<String, ROSStruct>,
    structs_by_type: &BTreeMap<String, ROSStruct>,
) -> TokenStream {
    let (schema_fn_call, schema_fn): (Vec<TokenStream>, Vec<TokenStream>) = structs_by_schema
        .values()
        .map(|ros_struct| {
            let schema_name = &ros_struct.schema_name;
            let type_underscore_name_schema =
                create_name_identity(&ros_struct.packaged_name, "_Schema");

            let fields = generate_arrow_schema_fields(
                schema_name,
                structs_by_schema,
                structs_by_type,
                "$",
                "",
                false,
            );

            let fn_call = quote!(
                #schema_name => #type_underscore_name_schema(),
            );

            let schema_fn = quote!(
                #[allow(non_snake_case)]
                pub fn #type_underscore_name_schema() -> Vec<Field> {
                    vec![#(#fields),*]
                }
            );

            (fn_call, schema_fn)
        })
        .unzip();

    let gen_function = quote! {

        #[allow(dead_code)]
        pub(crate) fn map_ros_schema_to_arrow_fields(ros_schema : &str) -> Vec<Field> {
            match ros_schema {
                #(#schema_fn_call)*
                unknown => {
                    log::warn!("Unknown schema {}, using binary parser.", unknown);
                    vec![Field::new("binary_data", DataType::LargeBinary, true)]
                }
            }
        }

        #(#schema_fn)*

    };

    gen_function
}

enum FieldType {
    StructArray(String),
    Struct(String),
    Primitive,
    PrimitiveVector,
}

fn rust_field_to_arrow_type_safe_token_stream(
    field_name: &str,
    field_path: &str,
    typ: &str,
    field_type: FieldType,
    flat: bool,
    index: &mut usize,
) -> ArrowSchemaField {
    let builder_field_name = create_name_identity(field_name, "");
    let path_field_name = parse_str::<syn::Expr>(field_path).unwrap();

    let (schema_suffix, struct_builder_suffix) = if flat {
        ("_FlatSchema", "_FlatStructBuilder")
    } else {
        ("_Schema", "_StructBuilder")
    };

    let (builder_type, builder_instantiation, builder_append, struct_builder_append) =
        match field_type {
            FieldType::Struct(underlying_type_name_str) => {
                let type_schema_fn_ident =
                    create_name_identity(underlying_type_name_str.as_str(), schema_suffix);
                let type_struct_builder_fn_ident =
                    create_name_identity(underlying_type_name_str.as_str(), struct_builder_suffix);

                let builder_type = quote!(arrow_array::builder::StructBuilder);
                let builder_instantiation =
                    quote!(arrow_array::builder::StructBuilder::from_fields(#type_schema_fn_ident(), 0));
                let builder_append = quote!(
                    let mut struct_builder = self.#builder_field_name.as_mut().unwrap();
                    #type_struct_builder_fn_ident(&msg.#path_field_name, struct_builder);
                    //self.#builder_field_name.as_mut().unwrap().append(true);
                );

                let struct_builder_append = quote!(
                    { // #path_field_name
                        let mut struct_builder = builder.field_builder::<arrow_array::builder::StructBuilder>(#index).unwrap();
                        #type_struct_builder_fn_ident(&msg.#path_field_name, &mut struct_builder);
                    }
                );

                (
                    builder_type,
                    builder_instantiation,
                    builder_append,
                    struct_builder_append,
                )
            }
            FieldType::StructArray(object_array_underscore_name) => {
                let type_schema_fn_ident =
                    create_name_identity(object_array_underscore_name.as_str(), schema_suffix);
                let type_struct_builder_fn_ident = create_name_identity(
                    object_array_underscore_name.as_str(),
                    struct_builder_suffix,
                );

                let builder_type =
                    quote!(arrow_array::builder::LargeListBuilder<arrow_array::builder::StructBuilder>);
                let builder_instantiation = quote!(arrow_array::builder::LargeListBuilder::new(
                    arrow_array::builder::StructBuilder::from_fields(#type_schema_fn_ident(), 0)
                ));
                let builder_append = quote!(
                    let mut struct_builder = self.#builder_field_name.as_mut().unwrap().values();
                    for element in msg.#path_field_name.iter() {
                        #type_struct_builder_fn_ident(element, &mut struct_builder);
                    }
                    self.#builder_field_name.as_mut().unwrap().append(true);
                );

                let struct_builder_append = quote!(
                    { // #path_field_name
                        let mut list_builder_option = builder.field_builder::<arrow_array::builder::LargeListBuilder<Box<dyn arrow_array::builder::ArrayBuilder>>>(#index);
                        let list_builder = list_builder_option.as_mut().unwrap();
                        let mut struct_builder : &mut arrow_array::builder::StructBuilder = list_builder.values().as_any_mut().downcast_mut::<arrow_array::builder::StructBuilder>().unwrap();
                        for element in msg.#path_field_name.iter() {
                            #type_struct_builder_fn_ident(element, &mut struct_builder);
                        }
                        list_builder.append(true);
                    }

                );

                (
                    builder_type,
                    builder_instantiation,
                    builder_append,
                    struct_builder_append,
                )
            }
            FieldType::PrimitiveVector => primitive_vector_builder_components(
                typ,
                &path_field_name,
                flat,
                &builder_field_name,
                index,
            ),
            FieldType::Primitive => {
                primitive_builder_components(typ, path_field_name, &builder_field_name, index)
            }
        };

    let builder_append = quote!(
        #field_name => {
            #builder_append;
        }
    );

    let builder_instantiation = quote!(
        #field_name => {
            this.#builder_field_name = Some(#builder_instantiation);
        }
    );

    let builder_finish = quote!(
        #field_name => res.push(Arc::new(self.#builder_field_name.as_mut().unwrap().finish())),
    );

    *index += 1;

    ArrowSchemaField {
        builder_field_name: quote!(#builder_field_name),
        builder_type,
        builder_instantiation,
        builder_append,
        builder_finish,
        struct_builder_append,
    }
}

fn primitive_vector_builder_components(
    typ: &str,
    path_field_name: &syn::Expr,
    flat: bool,
    builder_field_name: &Ident,
    index: &mut usize,
) -> (TokenStream, TokenStream, TokenStream, TokenStream) {
    let (builder_item_type, builder_item_instantiation, builder_append) = match typ {
        "Vec<bool>" => (
            quote!(arrow_array::builder::BooleanBuilder),
            quote!(arrow_array::builder::BooleanBuilder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val))),
        ),
        "Vec<str>" | "Vec<std::string::String>" => (
            quote!(arrow_array::builder::StringBuilder),
            quote!(arrow_array::builder::StringBuilder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(val.as_str()))),
        ),
        "Vec<char>" => (
            quote!(arrow_array::builder::StringBuilder),
            quote!(arrow_array::builder::StringBuilder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(val.to_string().as_str()))),
        ),
        "Vec<i8>" => (
            quote!(arrow_array::builder::Int8Builder),
            quote!(arrow_array::builder::Int8Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val))),
        ),
        "Vec<i16>" => (
            quote!(arrow_array::builder::Int16Builder),
            quote!(arrow_array::builder::Int16Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val))),
        ),
        "Vec<i32>" => (
            quote!(arrow_array::builder::Int32Builder),
            quote!(arrow_array::builder::Int32Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val))),
        ),
        "Vec<i64>" => (
            quote!(arrow_array::builder::Int64Builder),
            quote!(arrow_array::builder::Int64Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val))),
        ),
        "Vec<i128>" | "Vec<isize>" => (
            quote!(arrow_array::builder::Int64Builder),
            quote!(arrow_array::builder::Int64Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val as i64))), // Note: potential loss of data
        ),
        "Vec<u128>" | "Vec<usize>" => (
            quote!(arrow_array::builder::UInt64Builder),
            quote!(arrow_array::builder::UInt64Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val as u64))), // Note: potential loss of data
        ),
        "Vec<u8>" => (
            quote!(arrow_array::builder::LargeBinaryBuilder),
            quote!(arrow_array::builder::LargeBinaryBuilder::new()),
            quote!(msg.#path_field_name),
        ),
        "Vec<u16>" => (
            quote!(arrow_array::builder::UInt16Builder),
            quote!(arrow_array::builder::UInt16Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val))),
        ),
        "Vec<u32>" => (
            quote!(arrow_array::builder::UInt32Builder),
            quote!(arrow_array::builder::UInt32Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val))),
        ),
        "Vec<u64>" => (
            quote!(arrow_array::builder::UInt64Builder),
            quote!(arrow_array::builder::UInt64Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val))),
        ),
        "Vec<f32>" => (
            quote!(arrow_array::builder::Float32Builder),
            quote!(arrow_array::builder::Float32Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val))),
        ),
        "Vec<f64>" => (
            quote!(arrow_array::builder::Float64Builder),
            quote!(arrow_array::builder::Float64Builder::new()),
            quote!(msg.#path_field_name.iter().map(|val| Some(*val))),
        ),
        _ => panic!("Unsupported type: {}", typ),
    };
    if typ == "Vec<u8>" {
        (
            quote!(#builder_item_type),
            builder_item_instantiation,
            quote!(self.#builder_field_name.as_mut().unwrap().append_value(&#builder_append)),
            quote!(builder
                    .field_builder::<#builder_item_type>(#index)
                    .as_mut()
                    .unwrap()
                    .append_value(&#builder_append);
            ),
        )
    } else {
        wrap_primitive_vector_builder_components(
            flat,
            builder_item_type,
            builder_item_instantiation,
            builder_field_name,
            builder_append,
            index,
            path_field_name,
        )
    }
}

fn wrap_primitive_vector_builder_components(
    flat: bool,
    builder_item_type: TokenStream,
    builder_item_instantiation: TokenStream,
    builder_field_name: &Ident,
    builder_append: TokenStream,
    index: &mut usize,
    path_field_name: &syn::Expr,
) -> (TokenStream, TokenStream, TokenStream, TokenStream) {
    if flat {
        (
            quote!(arrow_array::builder::LargeListBuilder<#builder_item_type>),
            quote!(arrow_array::builder::LargeListBuilder::new(#builder_item_instantiation)),
            quote!(self.#builder_field_name.as_mut().unwrap().append_value(#builder_append)),
            quote!(builder
                    .field_builder::<arrow_array::builder::LargeListBuilder<#builder_item_type>>(#index)
                    .as_mut()
                    .unwrap()
                    .append_value(#builder_append);
            ),
        )
    } else {
        (
            quote!(arrow_array::builder::LargeListBuilder<#builder_item_type>),
            quote!(arrow_array::builder::LargeListBuilder::new(#builder_item_instantiation)),
            quote!(self.#builder_field_name.as_mut().unwrap().append_value(#builder_append)),
            quote!(
                {
                  let mut list_builder_option = builder.field_builder::<arrow_array::builder::LargeListBuilder<Box<dyn arrow_array::builder::ArrayBuilder>>>(#index);
                  let mut list_builder = list_builder_option.as_mut().unwrap();
                  let value_builder = list_builder.values().as_any_mut().downcast_mut::<#builder_item_type>().unwrap();
                  for value in msg.#path_field_name.iter() {
                    value_builder.append_value(value.clone());
                  }
                  list_builder.append(true);
                }
            ),
        )
    }
}

fn primitive_builder_components(
    typ: &str,
    path_field_name: syn::Expr,
    builder_field_name: &Ident,
    index: &mut usize,
) -> (TokenStream, TokenStream, TokenStream, TokenStream) {
    let (builder_item_type, builder_item_instantiation, builder_append) = match typ {
        "bool" => (
            quote!(arrow_array::builder::BooleanBuilder),
            quote!(arrow_array::builder::BooleanBuilder::new()),
            quote!(msg.#path_field_name),
        ),
        "str" | "std::string::String" => (
            quote!(arrow_array::builder::StringBuilder),
            quote!(arrow_array::builder::StringBuilder::new()),
            quote!(msg.#path_field_name.as_str()),
        ),
        "char" => (
            quote!(arrow_array::builder::StringBuilder),
            quote!(arrow_array::builder::StringBuilder::new()),
            quote!(msg.#path_field_name.to_string().as_str()),
        ),
        "()" => (
            quote!(arrow_array::builder::NullBuilder),
            quote!(arrow_array::builder::NullBuilder::new()),
            quote!(None::<()>),
        ),
        "i8" => (
            quote!(arrow_array::builder::Int8Builder),
            quote!(arrow_array::builder::Int8Builder::new()),
            quote!(msg.#path_field_name),
        ),
        "i16" => (
            quote!(arrow_array::builder::Int16Builder),
            quote!(arrow_array::builder::Int16Builder::new()),
            quote!(msg.#path_field_name),
        ),
        "i32" => (
            quote!(arrow_array::builder::Int32Builder),
            quote!(arrow_array::builder::Int32Builder::new()),
            quote!(msg.#path_field_name),
        ),
        "i64" => (
            quote!(arrow_array::builder::Int64Builder),
            quote!(arrow_array::builder::Int64Builder::new()),
            quote!(msg.#path_field_name),
        ),
        // Note: i128 and isize are mapped to Int64Builder with potential data loss
        "i128" | "isize" => (
            quote!(arrow_array::builder::Int64Builder),
            quote!(arrow_array::builder::Int64Builder::new()),
            quote!(msg.#path_field_name as i64),
        ),
        "u8" => (
            quote!(arrow_array::builder::UInt8Builder),
            quote!(arrow_array::builder::UInt8Builder::new()),
            quote!(msg.#path_field_name),
        ),
        "u16" => (
            quote!(arrow_array::builder::UInt16Builder),
            quote!(arrow_array::builder::UInt16Builder::new()),
            quote!(msg.#path_field_name),
        ),
        "u32" => (
            quote!(arrow_array::builder::UInt32Builder),
            quote!(arrow_array::builder::UInt32Builder::new()),
            quote!(msg.#path_field_name),
        ),
        "u64" => (
            quote!(arrow_array::builder::UInt64Builder),
            quote!(arrow_array::builder::UInt64Builder::new()),
            quote!(msg.#path_field_name),
        ),
        // Note: u128 and usize are mapped to UInt64Builder with potential data loss
        "u128" | "usize" => (
            quote!(arrow_array::builder::UInt64Builder),
            quote!(arrow_array::builder::UInt64Builder::new()),
            quote!(msg.#path_field_name as u64),
        ),
        "f32" => (
            quote!(arrow_array::builder::Float32Builder),
            quote!(arrow_array::builder::Float32Builder::new()),
            quote!(msg.#path_field_name),
        ),
        "f64" => (
            quote!(arrow_array::builder::Float64Builder),
            quote!(arrow_array::builder::Float64Builder::new()),
            quote!(msg.#path_field_name),
        ),
        _ => panic!("Unsupported type: {}", typ),
    };

    (
        quote!(#builder_item_type),
        builder_item_instantiation,
        quote!(self.#builder_field_name.as_mut().unwrap().append_value(#builder_append)),
        quote!(builder
            .field_builder::<#builder_item_type>(#index)
            .unwrap()
            .append_value(#builder_append);
        ),
    )
}

struct ArrowSchemaField {
    builder_field_name: TokenStream,
    builder_type: TokenStream,
    builder_instantiation: TokenStream,
    builder_append: TokenStream,
    builder_finish: TokenStream,
    struct_builder_append: TokenStream,
}

fn generate_arrow_schema_typesafe_parser_components(
    schema: &str,
    structs_by_schema: &BTreeMap<String, ROSStruct>,
    structs_by_type: &BTreeMap<String, ROSStruct>,
    parent_dotted_path: &str,
    parent_field: &str,
    index: &mut usize,
    flat: bool,
) -> Vec<ArrowSchemaField> {
    let ros_struct = structs_by_schema.get(schema).unwrap();
    let mut arrows_schema_fields: Vec<ArrowSchemaField> = vec![];
    for field in &ros_struct.fields {
        let field_name = if !parent_field.is_empty() {
            format!("{}_{}", parent_field, field.name.clone())
        } else {
            field.name.clone()
        };

        let dotted_path = if !parent_dotted_path.is_empty() {
            format!("{}.{}", parent_dotted_path, field.name.clone())
        } else {
            field.name.clone()
        };

        let mut typ: Vec<ArrowSchemaField> = match field.native_type.as_str() {
            "bool"
            | "str"
            | "char"
            | "()"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "isize"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "usize"
            | "f32"
            | "f64"
            | "std::string::String" => {
                vec![rust_field_to_arrow_type_safe_token_stream(
                    &field_name,
                    &dotted_path,
                    field.native_type.as_str(),
                    FieldType::Primitive,
                    flat,
                    index,
                )]
            }
            "Vec<bool>"
            | "Vec<str>"
            | "Vec<char>"
            | "Vec<()>"
            | "Vec<i8>"
            | "Vec<i16>"
            | "Vec<i32>"
            | "Vec<i64>"
            | "Vec<i128>"
            | "Vec<isize>"
            | "Vec<u8>"
            | "Vec<u16>"
            | "Vec<u32>"
            | "Vec<u64>"
            | "Vec<u128>"
            | "Vec<usize>"
            | "Vec<f32>"
            | "Vec<f64>"
            | "Vec<std::string::String>" => {
                vec![rust_field_to_arrow_type_safe_token_stream(
                    &field_name,
                    &dotted_path,
                    field.native_type.as_str(),
                    FieldType::PrimitiveVector,
                    flat,
                    index,
                )]
            }
            typ if flat && !typ.starts_with("Vec") => {
                let typ = format!("r2r::{}", typ);
                println!("{}", typ);
                let field_struct = structs_by_type.get(&typ).unwrap();
                generate_arrow_schema_typesafe_parser_components(
                    &field_struct.schema_name,
                    structs_by_schema,
                    structs_by_type,
                    &dotted_path,
                    &field_name,
                    index,
                    flat,
                )
            }
            typ if !flat && !typ.starts_with("Vec") => {
                let typ = format!("r2r::{}", typ);
                let field_struct = structs_by_type.get(&typ).unwrap();

                let type_underscore_name_str =
                    field_struct.packaged_name.replace("::", "_").to_string();

                vec![rust_field_to_arrow_type_safe_token_stream(
                    &field_name,
                    &dotted_path,
                    typ.as_str(),
                    FieldType::Struct(type_underscore_name_str),
                    flat,
                    index,
                )]
            }
            typ => {
                let typ = &typ[4..typ.len() - 1];
                let typ = format!("r2r::{}", typ);
                let field_struct = structs_by_type.get(&typ).unwrap();

                let type_underscore_name_str =
                    field_struct.packaged_name.replace("::", "_").to_string();

                vec![rust_field_to_arrow_type_safe_token_stream(
                    &field_name,
                    &dotted_path,
                    typ.as_str(),
                    FieldType::StructArray(type_underscore_name_str),
                    flat,
                    index,
                )]
            }
        };
        arrows_schema_fields.append(&mut typ);
    }
    arrows_schema_fields
}

#[allow(dead_code)]
fn generate_arrow_flat_rowbuilders(
    structs_by_schema: &BTreeMap<String, ROSStruct>,
    structs_by_type: &BTreeMap<String, ROSStruct>,
) -> TokenStream {
    let instantiation_and_row_appender: Vec<(TokenStream, TokenStream)> = structs_by_schema
        .values()
        .map(|ros_struct| {
            let schema_name = &ros_struct.schema_name;
            let type_name_str = &ros_struct.packaged_name;
            let type_name: syn::Path = parse_str::<syn::Path>(type_name_str).unwrap();
            let type_underscore_name_str = create_name(&ros_struct.packaged_name, "_RowBuilder");
            let type_underscore_name = create_name_identity( &type_underscore_name_str, "");
            let flat_struct_builder_fn_ident = create_name_identity(&ros_struct.packaged_name, "_FlatStructBuilder");
            let struct_builder_fn_ident = create_name_identity(&ros_struct.packaged_name, "_StructBuilder");
            let struct_schema_fn_ident = create_name_identity(&ros_struct.packaged_name, "_Schema");
            let flat_schema_fn_ident = create_name_identity(&ros_struct.packaged_name, "_FlatSchema");
   
            
            let fields = generate_arrow_schema_typesafe_parser_components(
                schema_name,
                structs_by_schema,
                structs_by_type,
                "",
                "",
                &mut 0,
                true,
            );

            let struct_fields = generate_arrow_schema_typesafe_parser_components(
                schema_name,
                structs_by_schema,
                structs_by_type,
                "",
                "",
                &mut 0,
                false,
            );


            let instantion = quote!(
                #schema_name => Box::new(#type_underscore_name::new(fields)),
            );

            let flat_struct_builder_appends: Vec<&TokenStream> =
                fields.iter().map(|field| &field.struct_builder_append).collect(); 

            let struct_builder_appends: Vec<&TokenStream> =
                struct_fields.iter().map(|field| &field.struct_builder_append).collect(); 


            let builder_field_definitions: Vec<TokenStream> = fields
                .iter()
                .map(|field| {
                    let builder_field_name = &field.builder_field_name;
                    let builder_type = &field.builder_type;
                    quote!(
                        #builder_field_name: Option<#builder_type>,

                    )
                })
                .collect();

            // builder_field_definitions.push(quote! {
            //     message_struct: Option<arrow_array::StructBuilder>
            // });

            let builder_field_init: Vec<TokenStream> = fields
                .iter()
                .map(|field| {
                    let builder_field_name = &field.builder_field_name;
                    quote!(
                        #builder_field_name: None,

                    )
                })
                .collect();
            // builder_field_init.push(quote! {
            //     message_struct: None,
            // });

            let builder_instantiation: Vec<&TokenStream> = fields
                .iter()
                .map(|field| &field.builder_instantiation)
                .collect();
            
            // let self_struct_builder_instantiation = quote! {
            //     message_struct: arrow_array::StructBuilder::from_fields(#schema_fn_ident(false)),
            // };
            // builder_instantiation.push(&self_struct_builder_instantiation);

            let builder_append: Vec<&TokenStream> =
                fields.iter().map(|field| &field.builder_append).collect();
            // let self_struct_builder_append = quote!{
            //     "message_struct" => #struct_builder_fn_ident(&msg, &mut self.message_struct.as_mut().unwrap()),
            // };
            // builder_append.push(&self_struct_builder_append);

            let builder_finish: Vec<&TokenStream> =
                fields.iter().map(|field| &field.builder_finish).collect();
            // let self_struct_builder_append = quote!{
            //     "message_struct" => res.push(Arc::new(self.message_struct.as_mut().unwrap().finish())),
            // };
            // builder_finish.push(&self_struct_builder_append);

            (
                instantion,
                quote!(
                    
                    impl<'a> ArrowSupport<'a> for #type_name {
                        type RowBuilderType = #type_underscore_name<'a>;

                        fn new_row_builder(arrow_fields: Vec<&'a Field>) -> Self::RowBuilderType {
                            Self::RowBuilderType::new(arrow_fields)
                        }

                        fn arrow_fields(include_msg_struct: bool) -> Vec<Field> {
                            #flat_schema_fn_ident(include_msg_struct)
                        }

                        fn arrow_schema(include_msg_struct: bool) -> Schema {
                            Schema::new(Self::arrow_fields(include_msg_struct))
                        }
                    }

                    #[allow(non_camel_case_types)]
                    pub struct #type_underscore_name<'a> {
                        _arrow_fields: Vec<&'a Field>,
                        #(#builder_field_definitions)*
                        message_struct: Option<arrow_array::builder::StructBuilder>,
                        _phantom: std::marker::PhantomData<&'a ()>,
                    }

                    impl<'a> #type_underscore_name<'a> {

                        pub fn deserialize(ser_msg : &[u8]) -> r2r::Result<#type_name> {
                            log::trace!("Deserializing bytes to {} in {}", #type_name_str, #type_underscore_name_str);
                            #type_name::from_serialized_bytes(ser_msg)
                        }

                        pub fn new(_arrow_fields: Vec<&'a Field>) -> Self {
                            log::debug!("Instantiating parser for {}: {}::new", #type_name_str, #type_underscore_name_str);
                            #[allow(unused_mut)]
                            let mut this = Self {
                                _arrow_fields,
                                message_struct: None,
                                #(#builder_field_init)*
                                _phantom: std::marker::PhantomData,
                            };

                            #[allow(unused)]
                            for field in &this._arrow_fields {
                                match field.name().as_str() {
                                    #(#builder_instantiation)*
                                    "message_struct" => {
                                        this.message_struct = Some(arrow_array::builder::StructBuilder::from_fields(#struct_schema_fn_ident(), 0)) 
                                    },
                                    other => log::error!("Invalid field name: {}", other)
                                }
                            }
                            this
                        }

                    }

                    impl<'a> RowBuilder<'a, #type_name> for #type_underscore_name<'a> {

                        fn add_row(&mut self, msg : &#type_name) -> Result<()> {
                            #[allow(unused)]
                            for field in &self._arrow_fields {
                                match field.name().as_str() {
                                    #(#builder_append),*
                                    "message_struct" => #struct_builder_fn_ident(&msg, &mut self.message_struct.as_mut().unwrap()),
                                    other => log::error!("Invalid field name: {}", other)
                                }
                            }
                            Ok(())
                        }

                        fn add_raw_row(&mut self, msg : &[u8]) -> Result<()> {
                            log::debug!("Adding row in {}", #type_underscore_name_str);
                            #[allow(unused)]
                            let msg = Self::deserialize(msg)?;
                            self.add_row(&msg)?;
                            Ok(())
                        }

                        fn to_arc_arrays(&mut self) -> Vec<Arc<dyn Array>> {
                            log::debug!("Building batch in {}", #type_underscore_name_str);
                            #[allow(unused_mut)]
                            let mut res : Vec<Arc<dyn Array>> = vec![];

                            #[allow(unused)]
                            for field in &self._arrow_fields {
                                match field.name().as_str() {
                                    #(#builder_finish)*
                                    "message_struct" => res.push(Arc::new(self.message_struct.as_mut().unwrap().finish())),
                                    other => log::error!("Invalid field name: {}", other)
                                }
                            }
                            res
                        }
                    }

                    #[allow(non_snake_case,unused)]
                    pub fn #flat_struct_builder_fn_ident(msg : &#type_name, builder: &mut arrow_array::builder::StructBuilder) {
                        #(#flat_struct_builder_appends)*
                        builder.append(true);
                    }

                    #[allow(non_snake_case,unused)]
                    pub fn #struct_builder_fn_ident(msg : &#type_name, builder: &mut arrow_array::builder::StructBuilder) {
                        #(#struct_builder_appends)*
                        builder.append(true);
                    }
                ),
            )
        })
        .collect();

    let (_, row_appenders): (Vec<TokenStream>, Vec<TokenStream>) =
        instantiation_and_row_appender.into_iter().unzip();

    let gen_function = quote! {

        // pub(crate) fn new_row_builder_for_schema<'a>(ros_schema : &str, fields: Vec<&'a Field>) -> Box<dyn RowBuilder<'a, T> + 'a> {
        //     match ros_schema {
        //         #(#instantiations)*
        //         unsupported_schema => {
        //             log::warn!("Unsupported schema: {}", unsupported_schema);
        //             panic!("Unsupported schema: {}", unsupported_schema);
        //             //Box::new(RawMessageRowBuilder::new(fields))
        //         },
        //     }
        // }

       #(#row_appenders)*
    };

    gen_function
}


fn generate_arrow_rowbuilders(
    structs_by_schema: &BTreeMap<String, ROSStruct>,
    structs_by_type: &BTreeMap<String, ROSStruct>,
) -> TokenStream {
    let instantiation_and_row_appender: Vec<(TokenStream, TokenStream)> = structs_by_schema
        .values()
        .map(|ros_struct| {
            let schema_name = &ros_struct.schema_name;
            let type_name_str = &ros_struct.packaged_name;
            let type_name: syn::Path = parse_str::<syn::Path>(type_name_str).unwrap();
            let type_underscore_name_str = create_name(&ros_struct.packaged_name, "_RowBuilder");
            let type_underscore_name = create_name_identity( &type_underscore_name_str, "");
            let flat_struct_builder_fn_ident = create_name_identity(&ros_struct.packaged_name, "_FlatStructBuilder");
            let struct_builder_fn_ident = create_name_identity(&ros_struct.packaged_name, "_StructBuilder");
            let struct_schema_fn_ident = create_name_identity(&ros_struct.packaged_name, "_Schema");
            //let flat_schema_fn_ident = create_name_identity(&ros_struct.packaged_name, "_FlatSchema");
   
            
            let fields = generate_arrow_schema_typesafe_parser_components(
                schema_name,
                structs_by_schema,
                structs_by_type,
                "",
                "",
                &mut 0,
                false,
            );

            let struct_fields = generate_arrow_schema_typesafe_parser_components(
                schema_name,
                structs_by_schema,
                structs_by_type,
                "",
                "",
                &mut 0,
                false,
            );


            let instantion = quote!(
                #schema_name => Box::new(#type_underscore_name::new(fields)),
            );

            let flat_struct_builder_appends: Vec<&TokenStream> =
                fields.iter().map(|field| &field.struct_builder_append).collect(); 

            let struct_builder_appends: Vec<&TokenStream> =
                struct_fields.iter().map(|field| &field.struct_builder_append).collect(); 


            let builder_field_definitions: Vec<TokenStream> = fields
                .iter()
                .map(|field| {
                    let builder_field_name = &field.builder_field_name;
                    let builder_type = &field.builder_type;
                    quote!(
                        #builder_field_name: Option<#builder_type>,

                    )
                })
                .collect();

            // builder_field_definitions.push(quote! {
            //     message_struct: Option<arrow_array::StructBuilder>
            // });

            let builder_field_init: Vec<TokenStream> = fields
                .iter()
                .map(|field| {
                    let builder_field_name = &field.builder_field_name;
                    quote!(
                        #builder_field_name: None,

                    )
                })
                .collect();
            // builder_field_init.push(quote! {
            //     message_struct: None,
            // });

            let builder_instantiation: Vec<&TokenStream> = fields
                .iter()
                .map(|field| &field.builder_instantiation)
                .collect();
            
            // let self_struct_builder_instantiation = quote! {
            //     message_struct: arrow_array::StructBuilder::from_fields(#schema_fn_ident(false)),
            // };
            // builder_instantiation.push(&self_struct_builder_instantiation);

            let builder_append: Vec<&TokenStream> =
                fields.iter().map(|field| &field.builder_append).collect();
            // let self_struct_builder_append = quote!{
            //     "message_struct" => #struct_builder_fn_ident(&msg, &mut self.message_struct.as_mut().unwrap()),
            // };
            // builder_append.push(&self_struct_builder_append);

            let builder_finish: Vec<&TokenStream> =
                fields.iter().map(|field| &field.builder_finish).collect();
            // let self_struct_builder_append = quote!{
            //     "message_struct" => res.push(Arc::new(self.message_struct.as_mut().unwrap().finish())),
            // };
            // builder_finish.push(&self_struct_builder_append);

            (
                instantion,
                quote!(
                    
                    impl<'a> ArrowSupport<'a> for #type_name {
                        type RowBuilderType = #type_underscore_name<'a>;

                        fn new_row_builder(arrow_fields: Vec<&'a Field>) -> Self::RowBuilderType {
                            Self::RowBuilderType::new(arrow_fields)
                        }

                        fn arrow_fields() -> Vec<Field> {
                            #struct_schema_fn_ident()
                        }

                        fn arrow_schema() -> Schema {
                            Schema::new(Self::arrow_fields())
                        }
                    }

                    #[allow(non_camel_case_types)]
                    pub struct #type_underscore_name<'a> {
                        _arrow_fields: Vec<&'a Field>,
                        #(#builder_field_definitions)*
                        message_struct: Option<arrow_array::builder::StructBuilder>,
                        _phantom: std::marker::PhantomData<&'a ()>,
                    }

                    impl<'a> #type_underscore_name<'a> {

                        pub fn deserialize(ser_msg : &[u8]) -> r2r::Result<#type_name> {
                            log::trace!("Deserializing bytes to {} in {}", #type_name_str, #type_underscore_name_str);
                            #type_name::from_serialized_bytes(ser_msg)
                        }

                        pub fn new(_arrow_fields: Vec<&'a Field>) -> Self {
                            log::debug!("Instantiating parser for {}: {}::new", #type_name_str, #type_underscore_name_str);
                            #[allow(unused_mut)]
                            let mut this = Self {
                                _arrow_fields,
                                message_struct: None,
                                #(#builder_field_init)*
                                _phantom: std::marker::PhantomData,
                            };

                            #[allow(unused)]
                            for field in &this._arrow_fields {
                                match field.name().as_str() {
                                    #(#builder_instantiation)*
                                    "message_struct" => {
                                        this.message_struct = Some(arrow_array::builder::StructBuilder::from_fields(#struct_schema_fn_ident(), 0)) 
                                    },
                                    other => log::error!("Invalid field name: {}", other)
                                }
                            }
                            this
                        }

                    }

                    impl<'a> RowBuilder<'a, #type_name> for #type_underscore_name<'a> {

                        fn add_row(&mut self, msg : &#type_name) -> Result<()> {
                            #[allow(unused)]
                            for field in &self._arrow_fields {
                                match field.name().as_str() {
                                    #(#builder_append),*
                                    "message_struct" => #struct_builder_fn_ident(&msg, &mut self.message_struct.as_mut().unwrap()),
                                    other => log::error!("Invalid field name: {}", other)
                                }
                            }
                            Ok(())
                        }

                        fn add_raw_row(&mut self, msg : &[u8]) -> Result<()> {
                            log::debug!("Adding row in {}", #type_underscore_name_str);
                            #[allow(unused)]
                            let msg = Self::deserialize(msg)?;
                            self.add_row(&msg)?;
                            Ok(())
                        }

                        fn to_arc_arrays(&mut self) -> Vec<Arc<dyn Array>> {
                            log::debug!("Building batch in {}", #type_underscore_name_str);
                            #[allow(unused_mut)]
                            let mut res : Vec<Arc<dyn Array>> = vec![];

                            #[allow(unused)]
                            for field in &self._arrow_fields {
                                match field.name().as_str() {
                                    #(#builder_finish)*
                                    "message_struct" => res.push(Arc::new(self.message_struct.as_mut().unwrap().finish())),
                                    other => log::error!("Invalid field name: {}", other)
                                }
                            }
                            res
                        }
                    }

                    #[allow(non_snake_case,unused)]
                    pub fn #flat_struct_builder_fn_ident(msg : &#type_name, builder: &mut arrow_array::builder::StructBuilder) {
                        #(#flat_struct_builder_appends)*
                        builder.append(true);
                    }

                    #[allow(non_snake_case,unused)]
                    pub fn #struct_builder_fn_ident(msg : &#type_name, builder: &mut arrow_array::builder::StructBuilder) {
                        #(#struct_builder_appends)*
                        builder.append(true);
                    }
                ),
            )
        })
        .collect();

    let (_, row_appenders): (Vec<TokenStream>, Vec<TokenStream>) =
        instantiation_and_row_appender.into_iter().unzip();

    let gen_function = quote! {

        // pub(crate) fn new_row_builder_for_schema<'a>(ros_schema : &str, fields: Vec<&'a Field>) -> Box<dyn RowBuilder<'a, T> + 'a> {
        //     match ros_schema {
        //         #(#instantiations)*
        //         unsupported_schema => {
        //             log::warn!("Unsupported schema: {}", unsupported_schema);
        //             panic!("Unsupported schema: {}", unsupported_schema);
        //             //Box::new(RawMessageRowBuilder::new(fields))
        //         },
        //     }
        // }

       #(#row_appenders)*
    };

    gen_function
}

#[cfg(feature = "doc-only")]
fn main() -> Result<()> {
    Ok(())
}

#[cfg(not(feature = "doc-only"))]
fn main() -> Result<()> {
    // Parse the source code as a syn file
    use r2r_common::get_env_hash;
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_dir_path = Path::new(&out_dir);

    let deps_dir = out_dir_path.parent().unwrap().parent().unwrap();

    println!("Out dir: {}", &env::var("OUT_DIR").unwrap());
    let mut log_file =
        File::create(deps_dir.join("build_debug.log")).expect("Failed to create log file");

    let env_hash = get_env_hash();

    writeln!(log_file, "This is a debug message from build.rs")
        .expect("Failed to write to log file");

    writeln!(log_file, "Env hash: {}", r2r_common::get_env_hash())
        .expect("Failed to write to log file");
    let desired_trait = "WrappedTypesupport";

    let implementing_structs =
        find_implementing_structs(deps_dir, env_hash.as_str(), desired_trait);

    let (structs_by_schema, structs_by_type) =
        find_structs_by_schema_and_type(deps_dir, env_hash.as_str(), &implementing_structs);

    //let map_function = generate_map_function(&structs_by_schema);s
    generate_schema(
        out_dir_path,
        &structs_by_schema,
    // &structs_by_type,
        &mut log_file,
    )?;

    generate_arrow_mappers(out_dir, structs_by_schema, structs_by_type, &mut log_file)?;
    Ok(())
}

fn generate_arrow_mappers(
    out_dir: String,
    structs_by_schema: BTreeMap<String, ROSStruct>,
    structs_by_type: BTreeMap<String, ROSStruct>,
    log_file: &mut File,
) -> Result<(), anyhow::Error> {
    let output_path = Path::new(&out_dir).join("generated_arrow_mappers.rs");
    let arrow_imports = generate_arrow_imports();
    let flat_arrow_schema_gen = generate_flat_arrow_schema(&structs_by_schema, &structs_by_type);
    let arrow_schema_gen = generate_arrow_schema(&structs_by_schema, &structs_by_type);
    let typesafe_parsers =
    generate_arrow_rowbuilders(&structs_by_schema, &structs_by_type);
    writeln!(log_file, "Writing to {:?}", output_path.clone())
        .expect("Failed to write to log file");

    write_token_streams_to_file(
        &output_path,
        vec![
            SourceCode::TokenStream(arrow_imports),
            SourceCode::TokenStream(flat_arrow_schema_gen),
            SourceCode::TokenStream(arrow_schema_gen),
            SourceCode::TokenStream(typesafe_parsers),
        ],
    )?;
    Ok(())
}

fn generate_schema(
    out_dir_path: &Path,
    structs_by_schema: &BTreeMap<String, ROSStruct>,
    //structs_by_type: &BTreeMap<String, ROSStruct>,
    log_file: &mut File,
) -> Result<(), anyhow::Error> {
    let output_path = out_dir_path.join("generated_schema.rs");
    let supported_schema_list = generate_supported_schema_list(structs_by_schema);
    let imports = generate_imports();
    writeln!(log_file, "Writing to {:?}", output_path.clone())
        .expect("Failed to write to log file");
    write_token_streams_to_file(
        &output_path,
        vec![
            SourceCode::TokenStream(imports),
            SourceCode::TokenStream(supported_schema_list),
        ],
    )?;
    Ok(())
}

fn write_token_streams_to_file(file_path: &Path, token_streams: Vec<SourceCode>) -> Result<()> {
    //let _ = fs::remove_file(file_path); // not matching, we don't care if the the file doesnt exist

    let mut content = String::new();
    for token_stream in token_streams {
        content.push_str(&format!("{}\n", token_stream)); // Accumulate the content
    }

    fs::write(file_path, content)?;

    Command::new("rustfmt")
        .arg(file_path.to_str().unwrap()) // Convert the Path to a &str
        .output() // Execute the command and capture the output
        .expect("Failed to execute rustfmt");

    Ok(())
}

fn find_structs_by_schema_and_type(
    deps_dir: &Path,
    env_hash: &str,
    implementing_structs: &HashSet<String>,
) -> (BTreeMap<String, ROSStruct>, BTreeMap<String, ROSStruct>) {
    let mut structs_by_schema: BTreeMap<String, ROSStruct> = BTreeMap::new();
    let mut structs_by_type: BTreeMap<String, ROSStruct> = BTreeMap::new();
    for entry in WalkDir::new(deps_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e: &walkdir::DirEntry| {
            let path = e.path().to_str().unwrap();
            path.contains("r2r-")
                && path.contains(env_hash)
                && (path.ends_with("msgs.rs") || path.ends_with("interfaces.rs"))
        })
    {
        println!("Reading {:?}", entry.path());
        let file_content = fs::read_to_string(entry.path()).expect("Unable to read file");
        let syntax_tree = syn::parse_file(&file_content).expect("Unable to parse code");
        let file_name_no_ext = entry.file_name().to_string_lossy().replace(".rs", "");

        let mut schema_name_format = format!("{}/msg/", &file_name_no_ext);
        schema_name_format.push_str("{}");

        // Create a new StructVisitor
        let mut visitor = StructVisitor {
            schema_name_format,
            structs_by_schema: &mut structs_by_schema,
            structs_by_type: &mut structs_by_type,
            module_stack: vec!["r2r".to_string(), file_name_no_ext],
            valid_structs: implementing_structs,
        };

        visitor.visit_file(&syntax_tree);
    }
    (structs_by_schema, structs_by_type)
}

fn find_implementing_structs(
    deps_dir: &Path,
    env_hash: &str,
    desired_trait: &str,
) -> HashSet<String> {
    let mut implementing_structs: HashSet<String> = HashSet::new();

    for entry in WalkDir::new(deps_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e: &walkdir::DirEntry| {
            let path = e.path().to_str().unwrap();
            path.contains("r2r-")
                && path.contains(env_hash)
                && (path.ends_with("msgs.rs") || path.ends_with("interfaces.rs"))
        })
    {
        println!("Reading {:?}", entry.path());
        let file_content = fs::read_to_string(entry.path()).expect("Unable to read file");
        let syntax_tree = syn::parse_file(&file_content).expect("Unable to parse code");
        let file_name_no_ext = entry.file_name().to_string_lossy().replace(".rs", "");

        let mut schema_name_format = format!("{}/msg/", &file_name_no_ext);
        schema_name_format.push_str("{}");

        // Create a new StructVisitor
        let mut visitor = TraitImplVisitor {
            desired_trait,
            implementing_structs: &mut implementing_structs,
            module_stack: vec!["r2r".to_string(), file_name_no_ext],
        };

        visitor.visit_file(&syntax_tree);
    }
    implementing_structs
}
