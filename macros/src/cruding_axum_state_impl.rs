use std::collections::HashMap;

use proc_macro2::TokenStream;
use syn::{
    Data, DataStruct, DeriveInput, Error, Expr, Fields, Ident, Meta, MetaNameValue, Result, Token,
    Type, punctuated::Punctuated,
};

use crate::common::{parse_from_attr, parse_from_metas};

pub fn cruding_axum_state_impl(
    mut input: DeriveInput,
    attrs: Punctuated<Meta, Token![,]>,
) -> Result<TokenStream> {
    let mut config = Config::default();

    parse_from_metas(attrs, |meta| parse_macro_opts(meta, &mut config))?;

    let fields = match &mut input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(f),
            ..
        }) => &mut f.named,
        _ => {
            return Err(Error::new_spanned(
                &input,
                "#[crudable_axum_state] only supports structs with named fields",
            ));
        }
    };

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

    todo!()
}

#[derive(Default)]
struct Config {
    default_types: HandlerTypes,
    handlers: HashMap<String, (Type, HandlerTypes)>,
}

#[derive(Default)]
struct HandlerTypes {
    crudable: Option<syn::ExprPath>,
    cache: Option<syn::ExprPath>,
    source: Option<syn::ExprPath>,
    axum_ctx: Option<syn::ExprPath>,
    inner_ctx: Option<syn::ExprPath>,
    error: Option<syn::ExprPath>,
    column: Option<syn::ExprPath>,
}

macro_rules! err {
    ($item:ident) => {
        return Err(Error::new_spanned(
            $item,
            "Available attributes: crudable, cache, source, axum_ctx, inner_ctx, error, column",
        ))
    };
}

fn parse_handler_type(handler_type: MetaNameValue, handler_def: &mut HandlerTypes) -> Result<()> {
    let ident = handler_type.path.get_ident().map(|x| x.to_string());
    macro_rules! assign_type {
        ($($field:ident)+) => {
            match ident.as_ref().map(String::as_str) {
                $(
                    Some(stringify!($field)) => {
                        let Expr::Path(path) = handler_type.value else { err!(handler_type) };
                        handler_def. $field = Some(path);
                    }
                )+
                _ => err!(handler_type),
            }
        };
    }

    assign_type!(
        crudable
        cache
        source
        axum_ctx
        inner_ctx
        error
        column
    );

    Ok(())
}

fn parse_macro_opts(meta: Meta, config: &mut Config) -> Result<()> {
    match meta {
        Meta::NameValue(handler_type) => {
            parse_handler_type(handler_type, &mut config.default_types)?
        }
        _ => err!(meta),
    }

    Ok(())
}

fn parse_field_attrs(meta: Meta, field: (&Ident, &Type), config: &mut Config) -> Result<()> {
    let field_config = config
        .handlers
        .entry(field.0.to_string())
        .or_insert_with(|| (field.1.clone(), Default::default()));

    let Meta::List(meta_list) = meta else {
        err!(meta)
    };
    let Ok(metas) = meta_list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
    else {
        return Ok(());
    };

    for meta in metas {
        match meta {
            Meta::NameValue(handler_type) => parse_handler_type(handler_type, &mut field_config.1)?,
            _ => err!(meta),
        }
    }

    Ok(())
}
