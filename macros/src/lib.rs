use proc_macro::TokenStream;

mod item;
mod spider;

#[proc_macro_derive(Item, attributes(serde))]
pub fn item(input: TokenStream) -> TokenStream {
    item::expand(syn::parse_macro_input!(input as syn::DeriveInput)).into()
}

#[proc_macro_attribute]
pub fn spider(attr: TokenStream, item: TokenStream) -> TokenStream {
    spider::expand(attr, item)
}
