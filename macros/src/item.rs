use quote::quote;
use syn::punctuated::Punctuated;
use syn::{Data, DeriveInput, Fields, Meta, Token, parse_quote};

pub(crate) fn expand(input: DeriveInput) -> proc_macro2::TokenStream {
    match try_expand(input) {
        Ok(tokens) => tokens,
        Err(error) => error.to_compile_error(),
    }
}

fn try_expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    require_serde_flag(
        &input.attrs,
        "deny_unknown_fields",
        "#[derive(macros::Item)] requires #[serde(deny_unknown_fields)]",
        &input,
    )?;

    let fields = match &input.data {
        Data::Struct(item) => match &item.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &input,
                    "#[derive(macros::Item)] only supports structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "#[derive(macros::Item)] only supports structs",
            ));
        }
    };
    if let Some(field) = fields
        .iter()
        .find(|field| has_serde_flag(&field.attrs, "flatten"))
    {
        return Err(syn::Error::new_spanned(
            field,
            "#[derive(macros::Item)] does not support #[serde(flatten)]",
        ));
    }
    let state = fields
        .iter()
        .find(|field| field.ident.as_ref().is_some_and(|ident| ident == "state"))
        .ok_or_else(|| {
            syn::Error::new_spanned(
                &input,
                "#[derive(macros::Item)] requires a field named `state`",
            )
        })?;

    require_serde_flag(
        &state.attrs,
        "skip",
        "the Item `state` field requires #[serde(skip)]",
        state,
    )?;

    let ident = &input.ident;
    let type_generics = input.generics.split_for_impl().1;
    let mut generics = input.generics.clone();
    generics.make_where_clause().predicates.push(parse_quote! {
        #ident #type_generics:
            ::serde::Serialize
            + ::serde::de::DeserializeOwned
            + ::std::marker::Send
            + ::std::marker::Sync
            + 'static
    });
    let (impl_generics, _, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::spider::item::Item for #ident #type_generics #where_clause {
            fn from_values(
                values: ::spider::item::Values,
            ) -> ::std::result::Result<Self, ::spider::item::Error> {
                ::spider::item::deserialize(values)
            }

            fn state(&self) -> &::spider::item::State {
                &self.state
            }

            fn state_mut(&mut self) -> &mut ::spider::item::State {
                &mut self.state
            }
        }
    })
}

fn require_serde_flag(
    attrs: &[syn::Attribute],
    expected: &str,
    message: &str,
    tokens: &impl quote::ToTokens,
) -> syn::Result<()> {
    if has_serde_flag(attrs, expected) {
        Ok(())
    } else {
        Err(syn::Error::new_spanned(tokens, message))
    }
}

fn has_serde_flag(attrs: &[syn::Attribute], expected: &str) -> bool {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("serde"))
        .filter_map(|attr| {
            attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .ok()
        })
        .flatten()
        .any(|meta| matches!(meta, Meta::Path(path) if path.is_ident(expected)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn derive(source: &str) -> String {
        expand(syn::parse_str(source).unwrap()).to_string()
    }

    fn error(source: &str) -> String {
        try_expand(syn::parse_str(source).unwrap())
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn implements_item_contract_for_named_state() {
        let output = derive(
            r#"
            #[serde(deny_unknown_fields)]
            struct Article<T> {
                title: T,
                #[serde(skip)]
                state: spider::item::State,
            }
            "#,
        );

        assert!(output.contains("spider :: item :: Item for Article < T >"));
        assert!(output.contains("spider :: item :: deserialize"));
        assert!(output.contains("& self . state"));
        assert!(output.contains("& mut self . state"));
    }

    #[test]
    fn requires_unknown_field_rejection() {
        let output = error(
            r#"
            struct Article {
                #[serde(skip)]
                state: spider::item::State,
            }
            "#,
        );

        assert_eq!(
            output,
            "#[derive(macros::Item)] requires #[serde(deny_unknown_fields)]"
        );
    }

    #[test]
    fn requires_skipped_state() {
        let output = error(
            r#"
            #[serde(deny_unknown_fields)]
            struct Article {
                state: spider::item::State,
            }
            "#,
        );

        assert_eq!(output, "the Item `state` field requires #[serde(skip)]");
    }

    #[test]
    fn rejects_items_without_named_state() {
        let missing = derive(
            r#"
            #[serde(deny_unknown_fields)]
            struct Article { title: String }
            "#,
        );
        let tuple = derive(
            r#"
            #[serde(deny_unknown_fields)]
            struct Article(String);
            "#,
        );

        assert!(missing.contains("requires a field named `state`"));
        assert!(tuple.contains("only supports structs with named fields"));
    }

    #[test]
    fn rejects_flattened_business_fields() {
        let output = error(
            r#"
            #[serde(deny_unknown_fields)]
            struct Article {
                #[serde(flatten)]
                extra: std::collections::HashMap<String, serde_json::Value>,
                #[serde(skip)]
                state: spider::item::State,
            }
            "#,
        );

        assert_eq!(
            output,
            "#[derive(macros::Item)] does not support #[serde(flatten)]"
        );
    }
}
