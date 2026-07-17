use serde::de::DeserializeOwned;

use crate::net;

pub(crate) fn parse<T>(response: &net::Response) -> Result<T, net::Error>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(response.body().as_ref()).map_err(net::Error::from)
}
