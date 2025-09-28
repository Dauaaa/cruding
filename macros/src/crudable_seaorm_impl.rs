// crates/macros/src/lib.rs

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::Token;
use syn::punctuated::Punctuated;
use syn::{
    Data, DataStruct, DeriveInput, Error, Expr, Fields, Lit, Meta, MetaNameValue, Result, Type,
};

use crate::common::{parse_from_attr, parse_from_metas};

pub fn crudable_seaorm_impl(
    mut input: DeriveInput,
    attrs: Punctuated<Meta, Token![,]>,
) -> Result<TokenStream> {
    let mut config = Config {
        table_name: None,
        pkeys: vec![],
        impl_axum: false,
        mono_field: None,
    };

    parse_from_metas(attrs, |meta| parse_macro_opts(meta, &mut config))?;

    let (fields, struct_level_attrs) = match &mut input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(f),
            ..
        }) => (&mut f.named, &mut input.attrs),
        _ => {
            return Err(Error::new_spanned(
                &input,
                "#[crudable_seaorm] only supports structs with named fields",
            ));
        }
    };

    for attr in struct_level_attrs.iter() {
        parse_from_attr(attr.clone(), |meta| {
            Ok(parse_struct_attrs(meta, &mut config))
        })?;
    }
    struct_level_attrs.retain(|attr| {
        !(attr.path().is_ident("crudable") || attr.path().is_ident("crudable_seaorm"))
    });

    for f in fields {
        let ident = f.ident.clone().unwrap();
        let ty = f.ty.clone();

        for attr in f.attrs.clone() {
            parse_from_attr(attr, |meta| {
                parse_field_attrs(meta, (&ident, &ty), &mut config)
            })?;
        }
        f.attrs.retain(|attr| !attr.path().is_ident("crudable"));
    }

    if config.pkeys.is_empty() {
        return Err(Error::new_spanned(
            &input.ident,
            "No primary key fields detected (need #[sea_orm(primary_key)])",
        ));
    }
    let (mono_ident, mono_ty) = config
        .mono_field
        .as_ref()
        .ok_or_else(|| Error::new_spanned(&input.ident, "Missing #[crudable(mono)] on a field"))
        .clone()?;

    // Build Pkey type + expr preserving order of PK fields as declared.
    let (pkey_type_tokens, pkey_expr_tokens) = build_pkey_tokens(&config);

    // Names and helpers
    let struct_ident = input.ident.clone();
    let column_ident = Ident::new("Column", Span::call_site());

    // Axum helper: <SingularPascal(TableName)>Pkey (e.g., todos -> TodoPkey)
    let axum_impl = build_axum_impl(&config, &pkey_type_tokens, &pkey_expr_tokens, &struct_ident)?;

    // Generate code sections
    let column_partialeq_impl = quote! {
        impl PartialEq for #column_ident {
            fn eq(&self, other: &Self) -> bool {
                core::mem::discriminant(self) == core::mem::discriminant(other)
            }
        }
    };

    let crudable_impl = quote! {
        impl ::cruding::Crudable for #struct_ident {
            type Pkey = #pkey_type_tokens;
            type MonoField = #mono_ty;

            fn pkey(&self) -> Self::Pkey {
                #pkey_expr_tokens
            }

            fn mono_field(&self) -> Self::MonoField {
                self.#mono_ident
            }
        }
    };

    let expanded = quote! {
        #input
        #column_partialeq_impl
        #crudable_impl
        #axum_impl
    };

    Ok(expanded)
}

struct Config {
    table_name: Option<String>,
    pkeys: Vec<(Ident, Type)>,
    impl_axum: bool,
    mono_field: Option<(Ident, Type)>,
}

fn parse_struct_attrs(meta: Meta, config: &mut Config) {
    if meta.path().is_ident("sea_orm") {
        let Meta::List(meta_list) = meta else {
            return;
        };
        let Ok(metas) = meta_list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        else {
            return;
        };

        for meta in metas {
            let Meta::NameValue(MetaNameValue { path, value, .. }) = meta else {
                continue;
            };
            if !path.is_ident("table_name") {
                continue;
            }
            let Expr::Lit(syn::ExprLit {
                lit: Lit::Str(table_name),
                ..
            }) = value
            else {
                continue;
            };

            config.table_name = Some(table_name.value());
        }
    }
}

fn parse_macro_opts(meta: Meta, config: &mut Config) -> Result<()> {
    match meta {
        Meta::Path(path) => {
            let path = path.get_ident().map(|ident| ident.to_string());
            match path.as_ref().map(String::as_str) {
                Some("axum") => config.impl_axum = true,
                _ => return Err(Error::new_spanned(path, "Supported opts: axum")),
            }
        }
        _ => return Err(Error::new_spanned(meta, "Supported opts: axum")),
    }

    Ok(())
}

fn parse_field_attrs(meta: Meta, field: (&Ident, &Type), config: &mut Config) -> Result<()> {
    if meta.path().is_ident("sea_orm") {
        let Meta::List(meta_list) = meta else {
            return Ok(());
        };
        let Ok(metas) = meta_list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        else {
            return Ok(());
        };

        for meta in metas {
            let Meta::Path(path) = meta else { continue };
            if !path.is_ident("primary_key") {
                continue;
            }

            config.pkeys.push((field.0.clone(), field.1.clone()));
            return Ok(());
        }

        return Ok(());
    }

    if meta.path().is_ident("crudable") {
        let Meta::List(meta_list) = meta else {
            return Ok(());
        };
        let Ok(metas) = meta_list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        else {
            return Ok(());
        };

        for meta in metas {
            println!("HEY: {meta:?}");
            let Meta::Path(path) = meta else { continue };
            if !path.is_ident("mono") {
                continue;
            }

            config.mono_field = Some((field.0.clone(), field.1.clone()));
        }
    }

    Ok(())
}

fn build_pkey_tokens(config: &Config) -> (TokenStream, TokenStream) {
    let (ids, tys) = config.pkeys.iter().cloned().unzip::<_, _, Vec<_>, Vec<_>>();

    (quote! { (#(#tys),*) }, quote! { (#(self.#ids),*) })
}

fn build_axum_impl(
    config: &Config,
    pkey_type_tokens: &TokenStream,
    pkey_expr_tokens: &TokenStream,
    struct_ident: &Ident,
) -> Result<Option<TokenStream>> {
    if !config.impl_axum {
        return Ok(None);
    }

    let Some(table_name) = &config.table_name else {
        return Err(Error::new(Span::call_site(), "Missing sea_orm table_name"));
    };

    let pkey_de_ident = Ident::new(&pascal_case(&singularize(table_name)), Span::call_site());
    let pkey_de_struct_fields = config.pkeys.iter().map(|(id, ty)| quote! { pub #id: #ty });
    let from_impl = quote! {
        impl From<#pkey_de_ident> for #pkey_type_tokens {
            fn from(value: #pkey_de_ident) -> Self { #pkey_expr_tokens }
        }
    };
    Ok(Some(quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        pub struct #pkey_de_ident { #( #pkey_de_struct_fields, )* }
        #from_impl
        impl ::cruding::CrudableAxum for #struct_ident { type PkeyDe = #pkey_de_ident; }
    }))
}

fn singularize(s: &str) -> String {
    if s.ends_with('s') && s.len() > 1 {
        s[..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

fn pascal_case(s: &str) -> String {
    s.split('_')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut c = p.chars();
            match c.next() {
                Some(h) => h.to_ascii_uppercase().to_string() + c.as_str(),
                None => String::new(),
            }
        })
        .collect::<String>()
}
