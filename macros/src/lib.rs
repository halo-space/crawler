use proc_macro::TokenStream;

mod spider;

#[proc_macro_attribute]
pub fn spider(attr: TokenStream, item: TokenStream) -> TokenStream {
    spider::expand(attr, item)
}
