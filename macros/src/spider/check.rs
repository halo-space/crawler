use syn::{
    AngleBracketedGenericArguments, FnArg, GenericArgument, ImplItemFn, PathArguments, ReturnType,
    Type,
};

pub(super) fn is_trait_method(name: &str) -> bool {
    matches!(
        name,
        "name" | "allowed_domains" | "start_urls" | "start" | "index"
    )
}

pub(super) fn is_item_function(method: &ImplItemFn) -> bool {
    if method.sig.asyncness.is_none() || method.sig.inputs.len() != 2 {
        return false;
    }

    let mut inputs = method.sig.inputs.iter();
    let receiver_is_shared = matches!(
        inputs.next(),
        Some(FnArg::Receiver(receiver)) if receiver.reference.is_some() && receiver.mutability.is_none()
    );
    let item_is_typed = matches!(inputs.next(), Some(FnArg::Typed(_)));
    let result_is_valid = matches!(
        &method.sig.output,
        ReturnType::Type(_, output) if is_result(output)
    );

    receiver_is_shared && item_is_typed && result_is_valid
}

pub(super) fn is_handler(method: &ImplItemFn) -> bool {
    if method.sig.asyncness.is_none() || method.sig.inputs.len() != 2 {
        return false;
    }

    let mut inputs = method.sig.inputs.iter();
    let receiver_is_shared = matches!(
        inputs.next(),
        Some(FnArg::Receiver(receiver)) if receiver.reference.is_some() && receiver.mutability.is_none()
    );
    let response_is_valid = matches!(
        inputs.next(),
        Some(FnArg::Typed(argument)) if type_ends_with(argument.ty.as_ref(), "Response")
    );
    let result_is_valid = matches!(
        &method.sig.output,
        ReturnType::Type(_, output) if is_result(output)
    );

    receiver_is_shared && response_is_valid && result_is_valid
}

fn is_result(ty: &Type) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    let Some(result) = path.path.segments.last() else {
        return false;
    };
    let PathArguments::AngleBracketed(arguments) = &result.arguments else {
        return false;
    };

    result.ident == "Result" && result_arguments(arguments)
}

fn result_arguments(arguments: &AngleBracketedGenericArguments) -> bool {
    let mut arguments = arguments.args.iter();
    let ok_is_unit = matches!(
        arguments.next(),
        Some(GenericArgument::Type(Type::Tuple(tuple))) if tuple.elems.is_empty()
    );
    let error_is_error = matches!(
        arguments.next(),
        Some(GenericArgument::Type(error)) if is_error(error)
    );

    ok_is_unit && error_is_error && arguments.next().is_none()
}

fn is_error(ty: &Type) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    let segments = &path.path.segments;

    segments.len() == 1 && segments[0].ident == "Error"
        || segments.len() == 2 && segments[0].ident == "spider" && segments[1].ident == "Error"
}

fn type_ends_with(ty: &Type, expected: &str) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };

    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn method(source: &str) -> ImplItemFn {
        syn::parse_str(source).unwrap()
    }

    #[test]
    fn recognizes_only_async_shared_response_result_handlers() {
        assert!(is_handler(&method(
            "async fn detail(&self, response: spider::Response) -> Result<(), spider::Error> { Ok(()) }"
        )));
        assert!(!is_handler(&method(
            "fn detail(&self, response: spider::Response) -> Result<(), spider::Error> { Ok(()) }"
        )));
        assert!(!is_handler(&method(
            "async fn detail(&mut self, response: spider::Response) -> Result<(), spider::Error> { Ok(()) }"
        )));
        assert!(!is_handler(&method(
            "async fn detail(&self, url: String) -> Result<(), spider::Error> { Ok(()) }"
        )));
        assert!(!is_handler(&method(
            "async fn detail(&self, response: spider::Response) { let _ = response; }"
        )));
        assert!(!is_handler(&method(
            "async fn detail(&self, response: spider::Response) -> Result<String, spider::Error> { Ok(response.url) }"
        )));
        assert!(!is_handler(&method(
            "async fn detail(&self, response: spider::Response) -> Result<(), std::io::Error> { let _ = response; Ok(()) }"
        )));
    }
}
