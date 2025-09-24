use std::str::FromStr;

use axum::{extract::FromRequestParts, http::StatusCode, http::request::Parts};
use cruding_core::list::CrudingListParams;
use serde::{Deserialize, Deserializer};

pub struct AxumListParams<C>(pub CrudingListParams<C>);

impl<Column, S> FromRequestParts<S> for AxumListParams<Column>
where
    Column: FromStr + Send + Sync + 'static,
    S: Clone,
{
    type Rejection = (StatusCode, String);

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        Box::pin(async move {
            Ok(Self(
                cruding_qs_list_filter::parse_cruding_qs_list_filter(
                    urlencoding::decode(parts.uri.query().unwrap_or_default())
                        .map_err(|_| {
                            (
                                StatusCode::BAD_REQUEST,
                                "Failed to parse query string".to_string(),
                            )
                        })?
                        .as_ref(),
                    &|s: &str| Column::from_str(s).ok(),
                )
                .map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Failed to parse query string".to_string(),
                    )
                })?
                .1,
            ))
        })
    }
}

pub struct VecOrSingle<T>(pub Vec<T>);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for VecOrSingle<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum VecOrSingleInner<T> {
            One(T),
            Many(Vec<T>),
        }

        match VecOrSingleInner::<T>::deserialize(deserializer)? {
            VecOrSingleInner::One(x) => Ok(VecOrSingle(vec![x])),
            VecOrSingleInner::Many(v) => Ok(VecOrSingle(v)),
        }
    }
}
