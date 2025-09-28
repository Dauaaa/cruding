mod common;
mod crudable_seaorm_impl;
mod cruding_axum_state_impl;

use proc_macro::TokenStream;
use syn::{
    Attribute, DeriveInput, Meta, MetaList, Token,
    parse::{Parse, ParseBuffer, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

#[proc_macro_attribute]
pub fn cruding_axum_state(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as DeriveInput);
    let attrs = parse_macro_input!(attr with Punctuated::<syn::Meta, Token![,]>::parse_terminated);

    cruding_axum_state_impl::cruding_axum_state_impl(item, attrs)
        .unwrap()
        .into()
}

#[proc_macro_attribute]
pub fn crudable_seaorm(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as DeriveInput);
    let attr = parse_macro_input!(attr with Punctuated::<syn::Meta, Token![,]>::parse_terminated);

    crudable_seaorm_impl::crudable_seaorm_impl(item, attr)
        .unwrap()
        .into()
}
