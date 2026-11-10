use quote::quote;
use syn::{Fields, FieldsNamed, ItemStruct, parse_quote};

pub(super) fn structure(mut item: ItemStruct) -> proc_macro2::TokenStream {
    let ident = item.ident.clone();
    let generics = item.generics.clone();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let fields = match fields(&item) {
        Ok(fields) => fields,
        Err(error) => return error,
    };

    if let Err(error) = inject_tx(&mut item) {
        return error;
    }

    let constructor_args = fields.iter().map(|(ident, ty)| quote!(#ident: #ty));
    let constructor_fields = fields.iter().map(|(ident, _)| quote!(#ident));

    quote! {
        #item

        impl #impl_generics #ident #ty_generics #where_clause {
            pub fn new(
                #(#constructor_args,)*
            ) -> impl ::core::ops::FnOnce(::halo::harbor::Tx) -> Self {
                move |tx| Self {
                    #(#constructor_fields,)*
                    tx,
                }
            }
        }
    }
}

fn fields(item: &ItemStruct) -> Result<Vec<(syn::Ident, syn::Type)>, proc_macro2::TokenStream> {
    match &item.fields {
        Fields::Unit => Ok(Vec::new()),
        Fields::Named(named) => {
            let mut fields = Vec::new();

            for field in &named.named {
                let Some(ident) = field.ident.clone() else {
                    continue;
                };

                if ident == "tx" {
                    return Err(quote! {
                        compile_error!("#[harbor] reserves the tx field for framework injection");
                    });
                }

                fields.push((ident, field.ty.clone()));
            }

            Ok(fields)
        }
        Fields::Unnamed(_) => Err(quote! {
            compile_error!("#[harbor] does not support tuple structs");
        }),
    }
}

fn inject_tx(item: &mut ItemStruct) -> Result<(), proc_macro2::TokenStream> {
    match &mut item.fields {
        Fields::Unit => {
            let fields: FieldsNamed = parse_quote!({ tx: ::halo::harbor::Tx });
            item.fields = Fields::Named(fields);
            Ok(())
        }
        Fields::Named(fields) => {
            fields.named.push(parse_quote!(tx: ::halo::harbor::Tx));
            Ok(())
        }
        Fields::Unnamed(_) => Err(quote! {
            compile_error!("#[harbor] does not support tuple structs");
        }),
    }
}
