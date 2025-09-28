use syn::{Attribute, Meta, Result, Token, punctuated::Punctuated};

pub(crate) fn parse_from_attr(
    attr: Attribute,
    mut parser: impl FnMut(Meta) -> Result<()>,
) -> Result<()> {
    parser(attr.meta)
}

pub(crate) fn parse_from_metas(
    metas: Punctuated<Meta, Token![,]>,
    mut parser: impl FnMut(Meta) -> Result<()>,
) -> Result<()> {
    for meta in metas {
        parser(meta)?;
    }

    Ok(())
}
