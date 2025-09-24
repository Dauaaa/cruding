//! Copied from https://github.com/rust-bakery/nom/blob/main/examples/json2.rs and modified

use std::str::FromStr;

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::{tag, take},
    character::{
        anychar, char,
        complete::{digit0, digit1},
        multispace0, none_of,
    },
    combinator::{map, map_opt, map_res, opt, value, verify},
    error::{Error, ParseError},
    multi::{fold, separated_list0},
    sequence::{delimited, preceded, separated_pair},
};

type JsonValue = serde_json::Value;

fn boolean<'a>() -> impl Parser<&'a str, Output = bool, Error = Error<&'a str>> {
    alt((value(false, tag("false")), value(true, tag("true"))))
}

fn u16_hex<'a>() -> impl Parser<&'a str, Output = u16, Error = Error<&'a str>> {
    map_res(take(4usize), |s| u16::from_str_radix(s, 16))
}

fn unicode_escape<'a>() -> impl Parser<&'a str, Output = char, Error = Error<&'a str>> {
    map_opt(
        alt((
            // Not a surrogate
            map(
                verify(u16_hex(), |cp| !(0xD800..0xE000).contains(cp)),
                |cp| cp as u32,
            ),
            // See https://en.wikipedia.org/wiki/UTF-16#Code_points_from_U+010000_to_U+10FFFF for details
            map(
                verify(
                    separated_pair(u16_hex(), tag("\\u"), u16_hex()),
                    |(high, low)| (0xD800..0xDC00).contains(high) && (0xDC00..0xE000).contains(low),
                ),
                |(high, low)| {
                    let high_ten = (high as u32) - 0xD800;
                    let low_ten = (low as u32) - 0xDC00;
                    (high_ten << 10) + low_ten + 0x10000
                },
            ),
        )),
        // Could probably be replaced with .unwrap() or _unchecked due to the verify checks
        std::char::from_u32,
    )
}

fn character(input: &str) -> IResult<&str, char> {
    let (input, c) = none_of("\"").parse(input)?;
    if c == '\\' {
        alt((
            map_res(anychar, |c| {
                Ok(match c {
                    '"' | '\\' | '/' => c,
                    'b' => '\x08',
                    'f' => '\x0C',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    _ => return Err(()),
                })
            }),
            preceded(char('u'), unicode_escape()),
        ))
        .parse(input)
    } else {
        Ok((input, c))
    }
}

fn string<'a>() -> impl Parser<&'a str, Output = String, Error = Error<&'a str>> {
    delimited(
        char('"'),
        fold(0.., character, String::new, |mut string, c| {
            string.push(c);
            string
        }),
        char('"'),
    )
}

fn number<'a>() -> impl Parser<&'a str, Output = serde_json::Number, Error = Error<&'a str>> {
    let has_integer_part = |input: &'a str| {
        let (mut new_input, _) = digit1.parse(input)?;
        if !new_input.is_empty() {
            (new_input, _) = opt((char('.'), digit0)).parse(new_input)?;
        }

        Ok((new_input, &input[..(input.len() - new_input.len())]))
    };
    let no_integer_part = |input: &'a str| {
        let (new_input, _) = (char('.'), digit1).parse(input)?;

        Ok((new_input, &input[..(input.len() - new_input.len())]))
    };

    move |input: &'a str| {
        let (input, number_str) = alt((has_integer_part, no_integer_part)).parse(input)?;

        Ok((
            input,
            serde_json::Number::from_str(number_str).map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
            })?,
        ))
    }
}

fn ws<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, Output = O, Error = E>>(
    f: F,
) -> impl Parser<&'a str, Output = O, Error = E> {
    delimited(multispace0(), f, multispace0())
}

fn array<'a>() -> impl Parser<&'a str, Output = Vec<JsonValue>, Error = Error<&'a str>> {
    delimited(
        char('['),
        ws(separated_list0(ws(char(',')), json_value())),
        char(']'),
    )
}

fn object<'a>()
-> impl Parser<&'a str, Output = serde_json::Map<String, JsonValue>, Error = Error<&'a str>> {
    map(
        delimited(
            char('{'),
            ws(separated_list0(
                ws(char(',')),
                separated_pair(string(), ws(char(':')), json_value()),
            )),
            char('}'),
        ),
        |key_values| key_values.into_iter().collect(),
    )
}

fn json_value() -> JsonParser {
    JsonParser
}

struct JsonParser;

// the main Parser implementation is done explicitely on a real type,
// because haaving json_value return `impl Parser` would result in
// "recursive opaque type" errors
impl<'a> Parser<&'a str> for JsonParser {
    type Output = JsonValue;
    type Error = Error<&'a str>;

    fn process<OM: nom::OutputMode>(
        &mut self,
        input: &'a str,
    ) -> nom::PResult<OM, &'a str, Self::Output, Self::Error> {
        use serde_json::Value::*;

        let mut parser = alt((
            value(Null, tag("null")),
            map(boolean(), Bool),
            map(string(), String),
            map(number(), Number),
            map(array(), Array),
            map(object(), Object),
        ));

        parser.process::<OM>(input)
    }
}

pub fn json<'a>() -> impl Parser<&'a str, Output = JsonValue, Error = Error<&'a str>> {
    json_value()
}
