use std::fs;

use bits::actions::ActionError;
use chrono::{DateTime, Duration, NaiveTime, Utc};
use roxmltree::Document;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::coercion::{
    as_object, date_to_ymd, max_request_date, max_request_u32, parse_request_class_like,
    parse_request_time,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleReleased {
    pub path: String,
    #[serde(default)]
    pub now_rfc3339: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ScheduleCatalog {
    products: Vec<Product>,
}

#[derive(Debug, Clone)]
struct Product {
    class: String,
    stream: Option<String>,
    domain: Option<String>,
    time: Option<String>,
    step: Option<u32>,
    type_name: Option<String>,
    diss_step: Option<u32>,
    diss_domain: Option<String>,
    diss_type: Option<String>,
    release_time: NaiveTime,
    release_delta_day: i64,
}

impl ScheduleCatalog {
    pub fn from_path(path: &str) -> Result<Self, ActionError> {
        let raw = fs::read_to_string(path).map_err(|err| {
            ActionError::ConfigError(format!("failed to read schedule XML: {err}"))
        })?;
        Self::from_raw_xml(&raw)
    }

    pub fn from_raw_xml(raw: &str) -> Result<Self, ActionError> {
        let start = raw.find("<schedule").ok_or_else(|| {
            ActionError::ConfigError("schedule XML must contain a <schedule> root".into())
        })?;
        let xml = &raw[start..];
        let doc = Document::parse(xml).map_err(|err| {
            ActionError::ConfigError(format!("failed to parse schedule XML: {err}"))
        })?;

        let mut products = Vec::new();
        for node in doc
            .descendants()
            .filter(|node| node.has_tag_name("product"))
        {
            let text = |name: &str| {
                node.children()
                    .find(|child| child.has_tag_name(name))
                    .and_then(|child| child.text())
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string)
            };

            let release_time = text("release_time").ok_or_else(|| {
                ActionError::ConfigError("schedule product missing release_time".into())
            })?;
            let release_time = NaiveTime::parse_from_str(&release_time, "%H:%M:%S")
                .map_err(|err| ActionError::ConfigError(format!("invalid release_time: {err}")))?;

            products.push(Product {
                class: text("class").unwrap_or_default().to_lowercase(),
                stream: text("stream").map(|value| value.to_lowercase()),
                domain: text("domain").map(|value| value.to_lowercase()),
                time: text("time"),
                step: text("step").and_then(|value| value.parse::<u32>().ok()),
                type_name: text("type").map(|value| value.to_lowercase()),
                diss_step: text("diss_step").and_then(|value| value.parse::<u32>().ok()),
                diss_domain: text("diss_domain").map(|value| value.to_lowercase()),
                diss_type: text("diss_type").map(|value| value.to_lowercase()),
                release_time,
                release_delta_day: text("release_delta_day")
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0),
            });
        }

        Ok(Self { products })
    }

    pub fn assert_request_released(
        &self,
        request: &Value,
        now: DateTime<Utc>,
    ) -> Result<(), ActionError> {
        let request = as_object(request)?;
        let date = max_request_date(request.get("date").ok_or_else(|| missing_key("date"))?)?;
        let time = parse_request_time(request.get("time").ok_or_else(|| missing_key("time"))?)?;
        let step = max_request_u32(request.get("step").ok_or_else(|| missing_key("step"))?)?;
        let classes =
            parse_request_class_like(request.get("class").ok_or_else(|| missing_key("class"))?);
        let streams =
            parse_request_class_like(request.get("stream").ok_or_else(|| missing_key("stream"))?);
        let domains = request
            .get("domain")
            .map(parse_request_class_like)
            .unwrap_or_else(|| vec!["g".into()]);
        let types =
            parse_request_class_like(request.get("type").ok_or_else(|| missing_key("type"))?);

        for class in &classes {
            for stream in &streams {
                for domain in &domains {
                    for type_name in &types {
                        let product = self
                            .release_product(class, stream, domain, &time, step, type_name)
                            .ok_or_else(|| {
                                ActionError::ConfigError(format!(
                                    "No release time found for date: {}, class: {class}, stream: {stream}, domain: {domain}, time: {time}, step: {step:04}, type {type_name}",
                                    date_to_ymd(date)
                                ))
                            })?;

                        let release_date = date.and_time(product.release_time)
                            + Duration::days(product.release_delta_day);
                        let release_date = release_date.and_utc();
                        if now < release_date {
                            return Err(ActionError::ResourceError(format!(
                                "Data not released yet. Release time is {release_date}."
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn release_product(
        &self,
        class: &str,
        stream: &str,
        domain: &str,
        time: &str,
        step: u32,
        type_name: &str,
    ) -> Option<&Product> {
        let mut matching = self
            .products
            .iter()
            .filter(|product| product.matches(class, stream, domain, time, type_name))
            .collect::<Vec<_>>();
        matching.sort_by_key(|product| product.step.or(product.diss_step).unwrap_or(0));
        matching
            .into_iter()
            .filter(|product| product.step.or(product.diss_step).unwrap_or(360) <= step)
            .max_by_key(|product| product.step.or(product.diss_step).unwrap_or(0))
            .or_else(|| {
                self.products.iter().find(|product| {
                    product.matches(class, stream, domain, time, type_name)
                        && product.step.is_none()
                })
            })
    }
}

impl Product {
    fn matches(
        &self,
        class: &str,
        stream: &str,
        domain: &str,
        time: &str,
        type_name: &str,
    ) -> bool {
        self.class == class
            && self
                .stream
                .as_ref()
                .is_none_or(|candidate| contains_token(candidate, stream))
            && self.time.as_ref().is_none_or(|candidate| candidate == time)
            && self
                .domain
                .as_ref()
                .or(self.diss_domain.as_ref())
                .is_none_or(|candidate| candidate == domain)
            && self
                .type_name
                .as_ref()
                .or(self.diss_type.as_ref())
                .is_none_or(|candidate| contains_token(candidate, type_name))
    }
}

fn contains_token(candidate: &str, needle: &str) -> bool {
    candidate
        .split('/')
        .any(|token| token.eq_ignore_ascii_case(needle))
}

fn missing_key(key: &str) -> ActionError {
    ActionError::ConfigError(format!("Missing required key in request: '{key}'"))
}

impl ScheduleReleased {
    pub fn current_time(&self) -> Result<DateTime<Utc>, ActionError> {
        match &self.now_rfc3339 {
            Some(value) => DateTime::parse_from_rfc3339(value)
                .map(|value| value.with_timezone(&Utc))
                .map_err(|err| {
                    ActionError::ConfigError(format!("invalid now_rfc3339 override: {err}"))
                }),
            None => Ok(Utc::now()),
        }
    }
}
