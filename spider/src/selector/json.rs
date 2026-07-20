use serde::de::DeserializeOwned;

use crate::net;

pub(crate) fn parse<T>(response: &net::Response) -> Result<T, net::Error>
where
    T: DeserializeOwned,
{
    serde_json::from_str(&response.text()?).map_err(net::Error::from)
}
