use std::collections::HashMap;

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Data, DataStruct, DeriveInput, Error, Expr, Fields, Ident, Lit, LitStr, Meta, MetaNameValue,
    Result, Token, Type, punctuated::Punctuated, spanned::Spanned,
};

use crate::common::{parse_from_attr, parse_from_metas};

pub fn cruding_axum_state_impl(
    mut input: DeriveInput,
    attrs: Punctuated<Meta, Token![,]>,
) -> Result<TokenStream> {
    let mut config = Config::default();

    let attrs_span = attrs.span();
    parse_from_metas(attrs, |meta| parse_macro_opts(meta, &mut config))?;

    if config.default_types.crud_name.is_some() {
        return Err(Error::new(attrs_span, "can't set default crud_name"));
    }
    if config.default_types.crudable.is_some() {
        return Err(Error::new(attrs_span, "can't set default crudable"));
    }
    if config.default_types.column.is_some() {
        return Err(Error::new(attrs_span, "can't set default column"));
    }

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

    for f in fields.iter_mut() {
        let ident = f.ident.clone().unwrap();
        let ty = f.ty.clone();

        for attr in f.attrs.clone() {
            parse_from_attr(attr, |meta| {
                parse_field_attrs(meta, (&ident, &ty), &mut config)
            })?;
        }

        f.attrs
            .retain(|attr| !attr.path().is_ident("crudable_handler"));
    }

    let mut handler_impls = vec![];
    for f in fields {
        let ident = f.ident.clone().unwrap();
        let ident_str = ident.to_string();
        let Some((_, handler_types)) = config.handlers.remove(&ident_str) else {
            continue;
        };

        handler_impls.push(build_handler_impls(
            &input.ident,
            f.ident.clone().unwrap(),
            &config.default_types,
            &handler_types,
        )?);
    }

    let expanded = quote! {
        #input
        #(#handler_impls)*
    };

    Ok(expanded)
}

fn build_handler_impls(
    state_ident: &Ident,
    handler_ident: Ident,
    defaults: &HandlerTypes,
    cur: &HandlerTypes,
) -> Result<TokenStream> {
    let crudable = if let Some(crudable) = &cur.crudable {
        crudable
    } else if let Some(crudable) = &defaults.crudable {
        crudable
    } else {
        return Err(Error::new(
            handler_ident.span(),
            "No value set for crudable",
        ));
    };
    let source_handle = if let Some(source_handle) = &cur.source_handle {
        source_handle
    } else if let Some(source_handle) = &defaults.source_handle {
        source_handle
    } else {
        return Err(Error::new(
            handler_ident.span(),
            "No value set for source_handle",
        ));
    };
    let axum_ctx = if let Some(axum_ctx) = &cur.axum_ctx {
        axum_ctx
    } else if let Some(axum_ctx) = &defaults.axum_ctx {
        axum_ctx
    } else {
        return Err(Error::new(
            handler_ident.span(),
            "No value set for axum_ctx",
        ));
    };
    let inner_ctx = if let Some(inner_ctx) = &cur.inner_ctx {
        inner_ctx
    } else if let Some(inner_ctx) = &defaults.inner_ctx {
        inner_ctx
    } else {
        return Err(Error::new(
            handler_ident.span(),
            "No value set for inner_ctx",
        ));
    };
    let error = if let Some(error) = &cur.error {
        error
    } else if let Some(error) = &defaults.error {
        error
    } else {
        return Err(Error::new(handler_ident.span(), "No value set for error"));
    };
    // make optional
    let column = if let Some(column) = &cur.column {
        Some(column)
    } else if let Some(column) = &defaults.column {
        Some(column)
    } else {
        None
    };
    let crud_name = if let Some(crud_name) = &cur.crud_name {
        crud_name
    } else if let Some(crud_name) = &defaults.crud_name {
        crud_name
    } else {
        return Err(Error::new(
            handler_ident.span(),
            "No value set for crud_name",
        ));
    };
    let inner_ctx_method_name = if let Some(inner_ctx_method_name) = &cur.inner_ctx_method_name {
        inner_ctx_method_name
    } else if let Some(inner_ctx_method_name) = &defaults.inner_ctx_method_name {
        inner_ctx_method_name
    } else {
        return Err(Error::new(
            handler_ident.span(),
            "No value set for inner_ctx_method_name",
        ));
    };
    let new_source_handle_method_name = if let Some(new_source_handle_method_name) =
        &cur.new_source_handle_method_name
    {
        new_source_handle_method_name
    } else if let Some(new_source_handle_method_name) = &defaults.new_source_handle_method_name {
        new_source_handle_method_name
    } else {
        return Err(Error::new(
            handler_ident.span(),
            "No value set for new_source_handle_method_name",
        ));
    };
    let listing = if let Some(listing) = cur.listing {
        listing
    } else {
        defaults.listing.unwrap_or_default()
    };

    let required_impls = quote! {
        impl ::cruding::axum_api::state::CrudableAxumState<#crudable> for #state_ident {
            type AxumCtx = #axum_ctx;
            type InnerCtx = #inner_ctx;
            type SourceHandle = #source_handle;
            type Error = #error;

            // this will be used like Router::new().nest(CRUD_NAME, ...)
            const CRUD_NAME: &'static str = #crud_name;

            fn new_source_handle(&self) -> Self::SourceHandle {
                Self:: #new_source_handle_method_name(self)
            }

            fn inner_ctx(&self) -> Self::InnerCtx {
                Self:: #inner_ctx_method_name(self)
            }
        }

        impl ::cruding::handler::CrudableHandlerGetter<#crudable, Arc<(#axum_ctx, #inner_ctx)>, #source_handle, #error> for #state_ident {
            fn handler(
                &self,
            ) -> &dyn cruding::handler::CrudableHandler<
                    #crudable,
                    Arc<(#axum_ctx, #inner_ctx)>,
                    #source_handle,
                    #error,
                > {
                    self. #handler_ident .as_ref()
            }
        }
    };

    let getter_list_ext_impl = if listing {
        let Some(column) = column else {
            return Err(Error::new(handler_ident.span(), "No value set for column"));
        };

        Some(quote! {
            impl ::cruding::axum_api::state::CrudableAxumStateListExt<#crudable> for #state_ident {
                type Column = #column;
            }

            impl ::cruding::handler::CrudableHandlerGetterListExt<#crudable, Arc<(#axum_ctx, #inner_ctx)>, #source_handle, #error, #column> for #state_ident {
                fn handler_list(
                    &self,
                ) -> &dyn cruding::handler::CrudableHandlerListExt<
                        #crudable,
                        Arc<(#axum_ctx, #inner_ctx)>,
                        #source_handle,
                        #error,
                        #column
                    > {
                        self. #handler_ident .as_ref()
                }
            }
        })
    } else {
        None
    };

    Ok(quote! {
        #required_impls
        #getter_list_ext_impl
    })
}

#[derive(Default)]
struct Config {
    default_types: HandlerTypes,
    handlers: HashMap<String, (Type, HandlerTypes)>,
}

#[derive(Default)]
struct HandlerTypes {
    crudable: Option<syn::ExprPath>,
    source_handle: Option<syn::ExprPath>,
    axum_ctx: Option<syn::ExprPath>,
    inner_ctx: Option<syn::ExprPath>,
    error: Option<syn::ExprPath>,
    column: Option<syn::ExprPath>,

    crud_name: Option<LitStr>,
    inner_ctx_method_name: Option<Ident>,
    new_source_handle_method_name: Option<Ident>,

    listing: Option<bool>,
}

macro_rules! err {
    ($item:ident, $ctx:expr) => {
        return Err(Error::new_spanned(
            $item,
            format!(
                "Error: {}, Available attributes: crudable, source_handle, axum_ctx, \
                inner_ctx, error, column, crud_name, inner_ctx_method_name, \
                new_source_handle_method_name, listing",
                $ctx
            ),
        ))
    };
}

fn parse_handler_type(handler_type: MetaNameValue, handler_def: &mut HandlerTypes) -> Result<()> {
    let ident = handler_type.path.get_ident().map(|x| x.to_string());

    match ident.as_deref() {
        Some("crudable") => {
            let Expr::Path(path) = handler_type.value else {
                err!(handler_type, "parsing crudable attr")
            };
            handler_def.crudable = Some(path);
        }
        Some("source_handle") => {
            let Expr::Path(path) = handler_type.value else {
                err!(handler_type, "parsing source_handle attr")
            };
            handler_def.source_handle = Some(path);
        }
        Some("axum_ctx") => {
            let Expr::Path(path) = handler_type.value else {
                err!(handler_type, "parsing axum_ctx attr")
            };
            handler_def.axum_ctx = Some(path);
        }
        Some("inner_ctx") => {
            let Expr::Path(path) = handler_type.value else {
                err!(handler_type, "parsing inner_ctx attr")
            };
            handler_def.inner_ctx = Some(path);
        }
        Some("error") => {
            let Expr::Path(path) = handler_type.value else {
                err!(handler_type, "parsing error attr")
            };
            handler_def.error = Some(path);
        }
        Some("column") => {
            let Expr::Path(path) = handler_type.value else {
                err!(handler_type, "parsing column attr")
            };
            handler_def.column = Some(path);
        }
        Some("crud_name") => {
            let Expr::Lit(syn::ExprLit {
                lit: Lit::Str(lit_str),
                ..
            }) = handler_type.value
            else {
                err!(handler_type, "parsing crud_name attr")
            };
            handler_def.crud_name = Some(lit_str);
        }
        Some("inner_ctx_method_name") => {
            let Expr::Path(ref path) = handler_type.value else {
                err!(handler_type, "parsing inner_ctx_method_name attr")
            };
            let Some(ident) = path.path.get_ident().cloned() else {
                err!(handler_type, "parsing inner_ctx_method_name attr")
            };
            handler_def.inner_ctx_method_name = Some(ident);
        }
        Some("new_source_handle_method_name") => {
            let Expr::Path(ref path) = handler_type.value else {
                err!(handler_type, "parsing new_source_handle_method_name attr")
            };
            let Some(ident) = path.path.get_ident().cloned() else {
                err!(handler_type, "parsing new_source_handle_method_name attr")
            };
            handler_def.new_source_handle_method_name = Some(ident);
        }
        Some("listing") => {
            let Expr::Lit(syn::ExprLit {
                lit: Lit::Bool(b), ..
            }) = handler_type.value
            else {
                err!(handler_type, "parsing listing attr")
            };
            handler_def.listing = Some(b.value());
        }
        _ => err!(handler_type, "Invalid attribute"),
    }

    Ok(())
}

fn parse_macro_opts(meta: Meta, config: &mut Config) -> Result<()> {
    match meta {
        Meta::NameValue(handler_type) => {
            parse_handler_type(handler_type, &mut config.default_types)?
        }
        _ => err!(meta, "Invalid options for cruding_axum_state()"),
    }

    Ok(())
}

fn parse_field_attrs(meta: Meta, field: (&Ident, &Type), config: &mut Config) -> Result<()> {
    let field_config = config
        .handlers
        .entry(field.0.to_string())
        .or_insert_with(|| (field.1.clone(), Default::default()));

    let Meta::List(meta_list) = meta else {
        err!(
            meta,
            format!(
                "Parsing field attribute for {}. It must be a list of args.",
                field.0.to_string()
            )
        )
    };
    let Ok(metas) = meta_list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
    else {
        return Ok(());
    };

    for meta in metas {
        match meta {
            Meta::NameValue(handler_type) => parse_handler_type(handler_type, &mut field_config.1)?,
            _ => err!(
                meta,
                format!("parsing field attribute for {}", field.0.to_string())
            ),
        }
    }

    Ok(())
}
