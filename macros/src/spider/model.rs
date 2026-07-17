use quote::quote;
use syn::{Fields, FieldsNamed, ItemStruct, parse_quote};

pub(super) fn expand(mut item: ItemStruct) -> proc_macro2::TokenStream {
    let ident = item.ident.clone();
    let factory = syn::Ident::new(&format!("__{ident}Factory"), proc_macro2::Span::call_site());
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
    let factory_fields = fields.iter().map(|(ident, ty)| quote!(#ident: #ty));
    let constructor_fields = fields.iter().map(|(ident, _)| quote!(#ident));
    let build_fields = fields.iter().map(|(ident, _)| quote!(#ident: self.#ident));

    quote! {
        #item

        struct #factory #generics #where_clause {
            #(#factory_fields,)*
        }

        impl #impl_generics #ident #ty_generics #where_clause {
            pub fn new(
                #(#constructor_args,)*
            ) -> impl ::spider::SpiderFactory<Spider = Self> {
                #factory {
                    #(#constructor_fields,)*
                }
            }
        }

        impl #impl_generics ::spider::SpiderFactory for #factory #ty_generics #where_clause {
            type Spider = #ident #ty_generics;

            fn build(self, tx: ::spider::Tx) -> Self::Spider {
                #ident {
                    #(#build_fields,)*
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
                        compile_error!("#[spider] reserves the tx field for framework injection");
                    });
                }

                fields.push((ident, field.ty.clone()));
            }

            Ok(fields)
        }
        Fields::Unnamed(_) => Err(quote! {
            compile_error!("#[spider] does not support tuple structs");
        }),
    }
}

fn inject_tx(item: &mut ItemStruct) -> Result<(), proc_macro2::TokenStream> {
    match &mut item.fields {
        Fields::Unit => {
            let fields: FieldsNamed = parse_quote!({ tx: ::spider::Tx });
            item.fields = Fields::Named(fields);
            Ok(())
        }
        Fields::Named(fields) => {
            fields.named.push(parse_quote!(tx: ::spider::Tx));
            Ok(())
        }
        Fields::Unnamed(_) => Err(quote! {
            compile_error!("#[spider] does not support tuple structs");
        }),
    }
}
