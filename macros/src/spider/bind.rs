use std::collections::BTreeSet;

use quote::{ToTokens, quote};
use syn::visit_mut::{self, VisitMut};
use syn::{Expr, ImplItem, ItemImpl, LitStr, Meta, Type, parse_quote};

use super::check;

pub(super) fn expand(item: ItemImpl, item_type: Option<Type>) -> proc_macro2::TokenStream {
    if item.trait_.is_some() {
        return quote! {
            compile_error!("#[spider] expects a plain impl block, not a trait impl");
        };
    }

    let attrs = item.attrs;
    let self_ty = item.self_ty;
    let generics = item.generics;
    let where_clause = generics.where_clause.as_ref();
    let item_type = item_type.unwrap_or_else(|| parse_quote!(::spider::item::Map));

    let mut methods = Vec::new();
    let mut errors = Vec::new();

    let mut item_functions = BTreeSet::new();
    for item in item.items {
        match item {
            ImplItem::Fn(mut method) => {
                let marked = take_item_marker(&mut method, &mut errors);
                let name = method.sig.ident.to_string();
                if marked || name == "item" {
                    if !check::is_item_function(&method) {
                        errors.push(
                            syn::Error::new_spanned(
                                &method.sig,
                                "Item functions require `async fn(&self, Item) -> Result<(), Error>`",
                            )
                            .to_compile_error(),
                        );
                    }
                    item_functions.insert(name);
                }
                methods.push(method);
            }
            other => {
                let tokens = other.into_token_stream().to_string();
                errors.push(quote! {
                    compile_error!(concat!("#[spider] only supports methods in impl blocks: ", #tokens));
                });
            }
        }
    }

    let handlers = methods
        .iter()
        .filter(|method| {
            !item_functions.contains(method.sig.ident.to_string().as_str())
                && check::is_handler(method)
        })
        .map(|method| method.sig.ident.to_string())
        .collect::<BTreeSet<_>>();

    for method in &mut methods {
        Rewrite {
            handlers: &handlers,
        }
        .visit_block_mut(&mut method.block);
    }

    let wrappers = handlers.iter().map(|name| {
        let method = syn::Ident::new(name, proc_macro2::Span::call_site());
        let wrapper = wrapper(name);
        let call = if check::is_trait_method(name) {
            quote!(::spider::Spider::#method(spider, response))
        } else {
            quote!(spider.#method(response))
        };

        quote! {
            fn #wrapper<'a>(
                spider: &'a Self,
                response: ::spider::Response,
            ) -> ::spider::net::BoxFuture<'a> {
                Box::pin(#call)
            }
        }
    });
    let arms = handlers.iter().map(|name| {
        let wrapper = wrapper(name);
        let name = LitStr::new(name, proc_macro2::Span::call_site());
        quote! {
            #name => Some(::spider::net::Handler::new(#name, Self::#wrapper)),
        }
    });
    let item_wrappers = item_functions
        .iter()
        .filter(|name| name.as_str() != "item")
        .map(|name| {
            let method = syn::Ident::new(name, proc_macro2::Span::call_site());
            let wrapper = item_wrapper(name);
            quote! {
                fn #wrapper<'a>(
                    spider: &'a Self,
                    item: <Self as ::spider::Spider>::Item,
                ) -> ::spider::net::BoxFuture<'a> {
                    Box::pin(spider.#method(item))
                }
            }
        });
    let item_arms = item_functions
        .iter()
        .filter(|name| name.as_str() != "item")
        .map(|name| {
            let wrapper = item_wrapper(name);
            let name = LitStr::new(name, proc_macro2::Span::call_site());
            quote! {
                #name => Some(::spider::item::Function::new(#name, Self::#wrapper)),
            }
        });

    let mut trait_methods = Vec::new();
    let mut inherent_methods = Vec::new();

    for mut method in methods {
        let name = method.sig.ident.to_string();
        if check::is_trait_method(&name) || name == "item" && item_functions.contains(&name) {
            method.vis = syn::Visibility::Inherited;
            trait_methods.push(method);
        } else {
            inherent_methods.push(method);
        }
    }

    quote! {
        #(#errors)*

        #(#attrs)*
        impl #generics #self_ty #where_clause {
            #(#inherent_methods)*
            #(#wrappers)*
            #(#item_wrappers)*

            fn __spider_item_fn_item<'a>(
                spider: &'a Self,
                item: <Self as ::spider::Spider>::Item,
            ) -> ::spider::net::BoxFuture<'a> {
                Box::pin(::spider::Spider::item(spider, item))
            }
        }

        #(#attrs)*
        impl #generics ::spider::Spider for #self_ty #where_clause {
            type Item = #item_type;

            fn tx(&self) -> &::spider::Tx {
                &self.tx
            }

            fn handler(&self, node: &str) -> Option<::spider::net::Handler> {
                match node {
                    #(#arms)*
                    _ => None,
                }
            }

            fn item_fn(&self, name: &str) -> Option<::spider::item::Function<Self>> {
                match name {
                    "item" => Some(::spider::item::Function::new(
                        "item",
                        Self::__spider_item_fn_item,
                    )),
                    #(#item_arms)*
                    _ => None,
                }
            }

            #(#trait_methods)*
        }
    }
}

fn take_item_marker(
    method: &mut syn::ImplItemFn,
    errors: &mut Vec<proc_macro2::TokenStream>,
) -> bool {
    let mut marked = false;
    let mut attrs = Vec::with_capacity(method.attrs.len());
    for attr in std::mem::take(&mut method.attrs) {
        if !attr.path().is_ident("item") {
            attrs.push(attr);
            continue;
        }
        if !matches!(attr.meta, Meta::Path(_)) {
            errors.push(
                syn::Error::new_spanned(attr, "#[item] does not accept arguments")
                    .to_compile_error(),
            );
        } else if marked {
            errors
                .push(syn::Error::new_spanned(attr, "duplicate #[item] marker").to_compile_error());
        }
        marked = true;
    }
    method.attrs = attrs;
    marked
}

fn wrapper(name: &str) -> syn::Ident {
    syn::Ident::new(
        &format!("__spider_handler_{name}"),
        proc_macro2::Span::call_site(),
    )
}

fn item_wrapper(name: &str) -> syn::Ident {
    syn::Ident::new(
        &format!("__spider_item_fn_{name}"),
        proc_macro2::Span::call_site(),
    )
}

struct Rewrite<'a> {
    handlers: &'a BTreeSet<String>,
}

impl VisitMut for Rewrite<'_> {
    fn visit_expr_mut(&mut self, expr: &mut Expr) {
        if let Expr::MethodCall(call) = expr
            && call.method == "node"
            && call.args.len() == 1
            && let Some(argument) = call.args.first()
            && let Some(name) = self_path(argument)
            && self.handlers.contains(&name)
        {
            let name = LitStr::new(&name, proc_macro2::Span::call_site());
            call.args = parse_quote!(#name);
            visit_mut::visit_expr_mut(self, expr);
            return;
        }

        visit_mut::visit_expr_mut(self, expr);
    }
}

fn self_path(expr: &Expr) -> Option<String> {
    let Expr::Path(path) = expr else {
        return None;
    };

    if path.qself.is_some() || path.path.segments.len() != 2 {
        return None;
    }

    let mut segments = path.path.segments.iter();
    let first = segments.next()?;
    let second = segments.next()?;

    (first.ident == "Self").then(|| second.ident.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_business_method_visibility() {
        let item = syn::parse_str(
            "impl BookSpider {
                pub async fn detail(
                    &self,
                    response: spider::Response,
                ) -> Result<(), spider::Error> {
                    let _ = response;
                    Ok(())
                }

                pub fn name(&self) -> &str { \"book\" }

                pub async fn index(
                    &self,
                    response: spider::Response,
                ) -> Result<(), spider::Error> {
                    let _ = response;
                    Ok(())
                }
            }",
        )
        .unwrap();
        let file: syn::File = syn::parse2(expand(item, None)).unwrap();
        let impls = file
            .items
            .iter()
            .filter_map(|item| match item {
                syn::Item::Impl(item) => Some(item),
                _ => None,
            })
            .collect::<Vec<_>>();
        let inherent = impls.iter().find(|item| item.trait_.is_none()).unwrap();
        let spider_impl = impls.iter().find(|item| item.trait_.is_some()).unwrap();

        let detail = inherent
            .items
            .iter()
            .find_map(|item| match item {
                ImplItem::Fn(method) if method.sig.ident == "detail" => Some(method),
                _ => None,
            })
            .unwrap();
        assert!(matches!(detail.vis, syn::Visibility::Public(_)));

        for name in ["name", "index"] {
            let method = spider_impl
                .items
                .iter()
                .find_map(|item| match item {
                    ImplItem::Fn(method) if method.sig.ident == name => Some(method),
                    _ => None,
                })
                .unwrap();
            assert!(matches!(method.vis, syn::Visibility::Inherited));
        }
    }
}
