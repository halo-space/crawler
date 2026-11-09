//! AI selector that extracts one JSON object from the current Response.

use serde_json::Value;

use crate::{ai, net, selector};

const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;
const MAX_PROMPT_BYTES: usize = 1024 * 1024;
const OUTPUT_CONSTRAINT: &str = "\n\n输出约束：只能返回一个合法 JSON 对象；禁止返回数组、标量、Markdown 代码块或说明文字。必须遵循提取要求中给出的字段结构。";
const CONTENT_OPEN: &str = "\n\n以下是需要提取的页面内容：\n<content>\n";
const CONTENT_CLOSE: &str = "\n</content>";

pub(crate) async fn select(
    openai: Option<&ai::OpenAI>,
    response: &net::Response,
    expr: &str,
) -> Result<Value, selector::Error> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Err(selector::Error::Ai("prompt cannot be empty".to_string()));
    }
    let openai =
        openai.ok_or_else(|| selector::Error::Ai("AI provider is not configured".to_string()))?;
    if response.body().len() > MAX_RESPONSE_BODY_BYTES {
        return Err(response_body_too_large());
    }
    let body = response
        .text()
        .map_err(|error| selector::Error::Ai(error.to_string()))?;
    let content = openai
        .complete(prompt(expr, &body)?)
        .await
        .map_err(|error| selector::Error::Ai(error.to_string()))?;
    let value: Value = serde_json::from_str(&content).map_err(|error| {
        selector::Error::Ai(format!("model content is not valid JSON: {error}"))
    })?;
    if !value.is_object() {
        return Err(selector::Error::Ai(
            "model content must be a JSON object".to_string(),
        ));
    }
    Ok(value)
}

fn prompt(expr: &str, body: &str) -> Result<String, selector::Error> {
    let length = [
        expr.len(),
        OUTPUT_CONSTRAINT.len(),
        CONTENT_OPEN.len(),
        body.len(),
        CONTENT_CLOSE.len(),
    ]
    .into_iter()
    .try_fold(0_usize, usize::checked_add)
    .filter(|length| *length <= MAX_PROMPT_BYTES)
    .ok_or_else(prompt_too_large)?;

    let mut prompt = String::with_capacity(length);
    prompt.push_str(expr);
    prompt.push_str(OUTPUT_CONSTRAINT);
    prompt.push_str(CONTENT_OPEN);
    prompt.push_str(body);
    prompt.push_str(CONTENT_CLOSE);
    Ok(prompt)
}

fn response_body_too_large() -> selector::Error {
    selector::Error::Ai(format!(
        "AI response body exceeds the {MAX_RESPONSE_BODY_BYTES}-byte limit before character decoding"
    ))
}

fn prompt_too_large() -> selector::Error {
    selector::Error::Ai(format!(
        "AI prompt exceeds the {MAX_PROMPT_BYTES}-byte limit"
    ))
}

#[cfg(test)]
mod tests;
