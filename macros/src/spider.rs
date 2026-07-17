use proc_macro::TokenStream;

use syn::parse::{Parse, ParseStream};
use syn::{Ident, Item, Token, Type, parse_macro_input};

use quote::quote;

mod bind;
mod check;
mod model;

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    match parse_macro_input!(item as Item) {
        Item::Struct(item) if attr.is_empty() => model::expand(item).into(),
        Item::Struct(_) => quote! {
            compile_error!("#[spider] arguments belong on the impl block");
        }
        .into(),
        Item::Impl(item) => {
            let args = parse_macro_input!(attr as Args);
            bind::expand(item, args.item).into()
        }
        _ => quote! {
            compile_error!("#[spider] only supports structs and impl blocks");
        }
        .into(),
    }
}

#[derive(Default)]
struct Args {
    item: Option<Type>,
}

impl Parse for Args {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self::default());
        }
        let name: Ident = input.parse()?;
        if name != "item" {
            return Err(syn::Error::new(name.span(), "expected `item = Type`"));
        }
        input.parse::<Token![=]>()?;
        let item = input.parse()?;
        if !input.is_empty() {
            return Err(input.error("#[spider] only accepts `item = Type`"));
        }
        Ok(Self { item: Some(item) })
    }
}
