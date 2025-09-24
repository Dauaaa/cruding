pub mod json_parser;

use cruding_core::list::{
    CrudingListFilter, CrudingListFilterOperators, CrudingListPagination, CrudingListParams,
    CrudingListSort, CrudingListSortOrder,
};
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha0, char, digit1},
    combinator::eof,
    sequence::delimited,
};

pub fn parse_cruding_qs_list_filter<'a, Column>(
    mut input: &'a str,
    column_from_str: &impl Fn(&str) -> Option<Column>,
) -> IResult<&'a str, CrudingListParams<Column>> {
    const FILTER: &str = "filter";
    const SORT: &str = "sort";
    const PAGINATION: &str = "pagination";
    let mut params = CrudingListParams {
        filters: vec![],
        pagination: CrudingListPagination { page: 1, size: 100 },
        sorts: vec![],
    };

    while !input.is_empty() {
        let res = alt((tag(FILTER), tag(SORT), tag(PAGINATION))).parse(input)?;
        input = res.0;
        let list_option_type = res.1;

        match list_option_type {
            FILTER => {
                let res = parse_filter(input, &column_from_str)?;
                params.filters.push(res.1);
                input = res.0;
            }
            SORT => {
                let res = parse_sort(input, &column_from_str)?;
                params.sorts.push(res.1);
                input = res.0;
            }
            PAGINATION => {
                let res = parse_pagination(input, &mut params.pagination)?;
                input = res.0;
            }
            _ => unreachable!("tags checked above"),
        }

        let res = alt((tag("&"), eof)).parse(input)?;
        input = res.0;
    }

    Ok((input, params))
}

fn parse_filter<'a, Column>(
    input: &'a str,
    column_from_str: &impl Fn(&str) -> Option<Column>,
) -> IResult<&'a str, CrudingListFilter<Column>> {
    println!("{input}");
    let (input, column) = parse_column(input, column_from_str)?;
    println!("{input} Now op");

    let (input, op) = parse_filter_operator(input)?;
    println!("{input}");

    Ok((input, CrudingListFilter { column, op }))
}

fn parse_filter_operator<'a>(input: &'a str) -> IResult<&'a str, CrudingListFilterOperators> {
    let (input, op_str) = delimited(
        tag("["),
        move |input: &'a str| {
            println!("HELLO {input}");
            let (input, op) = alt((
                tag("="),
                tag("!="),
                tag(">="),
                tag(">"),
                tag("<="),
                tag("<"),
                tag("in"),
                tag("!in"),
            ))
            .parse(input)?;

            println!("HELLO {input}");

            Ok((input, op))
        },
        tag("]"),
    )
    .parse(input)?;

    println!("HELLO {input}");
    let (input, _) = char('=').parse(input)?;

    println!("NOW JSON {input}");
    let (input, op_rhs) = json_parser::json().parse(input)?;

    println!("HELLO {input}");
    let op = match op_str {
        "=" => CrudingListFilterOperators::Eq(op_rhs),
        "!=" => CrudingListFilterOperators::Neq(op_rhs),
        ">" => CrudingListFilterOperators::Gt(op_rhs),
        ">=" => CrudingListFilterOperators::Ge(op_rhs),
        "<" => CrudingListFilterOperators::Lt(op_rhs),
        "<=" => CrudingListFilterOperators::Le(op_rhs),
        "in" => {
            let serde_json::Value::Array(arr) = op_rhs else {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::SeparatedList,
                )));
            };
            CrudingListFilterOperators::In(arr)
        }
        "!in" => {
            let serde_json::Value::Array(arr) = op_rhs else {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::SeparatedList,
                )));
            };
            CrudingListFilterOperators::NotIn(arr)
        }
        _ => unreachable!("tags checked above"),
    };

    Ok((input, op))
}

fn parse_sort<'a, Column>(
    input: &'a str,
    column_from_str: &impl Fn(&str) -> Option<Column>,
) -> IResult<&'a str, CrudingListSort<Column>> {
    let (input, column) = parse_column(input, column_from_str)?;
    let (input, _) = char('=').parse(input)?;
    let (input, order_str) = alt((tag("asc"), tag("desc"))).parse(input)?;

    let order = match order_str {
        "asc" => CrudingListSortOrder::Asc,
        "desc" => CrudingListSortOrder::Desc,
        _ => unreachable!("tags checked above"),
    };

    Ok((input, CrudingListSort { column, order }))
}

fn parse_pagination<'a>(
    input: &'a str,
    pagination: &mut CrudingListPagination,
) -> IResult<&'a str, ()> {
    let (input, page_or_size) =
        delimited(tag("["), alt((tag("page"), tag("size"))), tag("]")).parse(input)?;
    let (input, _) = char('=').parse(input)?;
    let (input, number) = digit1.parse(input)?;

    match page_or_size {
        "page" => {
            pagination.page = number.parse().map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
            })?
        }
        "size" => {
            pagination.size = number.parse().map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
            })?
        }
        _ => unreachable!("tags checked above"),
    };

    Ok((input, ()))
}

fn parse_column<'a, C>(
    input: &'a str,
    column_from_str: &impl Fn(&str) -> Option<C>,
) -> IResult<&'a str, C> {
    delimited(
        tag("["),
        move |input: &'a str| {
            let (input, column) = alpha0(input)?;

            Ok((
                input,
                column_from_str(column).ok_or_else(|| {
                    nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
                })?,
            ))
        },
        tag("]"),
    )
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::error::ErrorKind;
    use serde_json::{Value, json};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Col {
        Foo,
        Bar,
    }

    fn map_col(s: &str) -> Option<Col> {
        match s {
            "foo" => Some(Col::Foo),
            "bar" => Some(Col::Bar),
            _ => None,
        }
    }

    // Small helpers so we can assert on private fields by pattern matching.
    fn assert_eq_filter<C>(
        f: &CrudingListFilter<C>,
        expect_col: &C,
        expect_op: &CrudingListFilterOperators,
    ) where
        C: std::fmt::Debug + PartialEq,
    {
        assert_eq!(
            &f.column, expect_col,
            "column mismatch: got {:?}, expect {:?}",
            f.column, expect_col
        );
        match (&f.op, expect_op) {
            (CrudingListFilterOperators::Eq(a), CrudingListFilterOperators::Eq(b)) => {
                assert_eq!(a, b)
            }
            (CrudingListFilterOperators::Neq(a), CrudingListFilterOperators::Neq(b)) => {
                assert_eq!(a, b)
            }
            (CrudingListFilterOperators::Gt(a), CrudingListFilterOperators::Gt(b)) => {
                assert_eq!(a, b)
            }
            (CrudingListFilterOperators::Ge(a), CrudingListFilterOperators::Ge(b)) => {
                assert_eq!(a, b)
            }
            (CrudingListFilterOperators::Lt(a), CrudingListFilterOperators::Lt(b)) => {
                assert_eq!(a, b)
            }
            (CrudingListFilterOperators::Le(a), CrudingListFilterOperators::Le(b)) => {
                assert_eq!(a, b)
            }
            (CrudingListFilterOperators::In(a), CrudingListFilterOperators::In(b)) => {
                assert_eq!(a, b)
            }
            (CrudingListFilterOperators::NotIn(a), CrudingListFilterOperators::NotIn(b)) => {
                assert_eq!(a, b)
            }
            (got, expect) => panic!("operator mismatch: got {:?}, expect {:?}", got, expect),
        }
    }

    #[test]
    fn parses_single_eq_number() {
        let input = "filter[foo][=]=1";
        let (rest, params) =
            parse_cruding_qs_list_filter::<Col>(input, &map_col).expect("parse ok");
        assert_eq!(rest, "");
        assert_eq!(params.filters.len(), 1);
        assert_eq!(params.sorts.len(), 0);
        assert_eq!(params.pagination.page, 1);
        assert_eq!(params.pagination.size, 100);

        let expected = CrudingListFilterOperators::Eq(json!(1));
        assert_eq_filter(&params.filters[0], &Col::Foo, &expected);
    }

    #[test]
    fn parses_all_ops_and_json_values() {
        let input = concat!(
            "filter[foo][=]=\"hi\"&",
            "filter[foo][!=]=null&",
            "filter[foo][>]=2&",
            "filter[foo][>=]=2&",
            "filter[foo][<]=3.14&",
            "filter[foo][<=]=true&",
            "filter[bar][in]=[1,2,3]&",
            "filter[bar][!in]=\"[\\\"x\\\",\\\"y\\\"]\"" // this is a string JSON, not array; test later too
        );

        // We expect this to fail on the last filter (!in with non-array). Parse piecewise for positive checks.
        let input_ok = "filter[foo][=]=\"hi\"&filter[foo][!=]=null&filter[foo][>]=2&filter[foo][>=]=2&filter[foo][<]=3.14&filter[foo][<=]=true&filter[bar][in]=[1,2,3]";
        let (_, params) =
            parse_cruding_qs_list_filter::<Col>(input_ok, &map_col).expect("parse ok");

        assert_eq!(params.filters.len(), 7);

        let expect = [
            (Col::Foo, CrudingListFilterOperators::Eq(json!("hi"))),
            (Col::Foo, CrudingListFilterOperators::Neq(Value::Null)),
            (Col::Foo, CrudingListFilterOperators::Gt(json!(2))),
            (Col::Foo, CrudingListFilterOperators::Ge(json!(2))),
            #[allow(clippy::approx_constant)]
            (Col::Foo, CrudingListFilterOperators::Lt(json!(3.14))),
            (Col::Foo, CrudingListFilterOperators::Le(json!(true))),
            (
                Col::Bar,
                CrudingListFilterOperators::In(vec![json!(1), json!(2), json!(3)]),
            ),
        ];

        for (i, (col, op)) in expect.iter().enumerate() {
            assert_eq_filter(&params.filters[i], col, op);
        }

        // Now assert the last (!in) with non-array fails with SeparatedList.
        let res = parse_cruding_qs_list_filter::<Col>(input, &map_col);
        match res {
            Err(nom::Err::Failure(e)) => assert_eq!(e.code, ErrorKind::SeparatedList),
            other => panic!("expected Failure(SeparatedList), got {:?}", other),
        }
    }

    #[test]
    fn parses_sorting_and_pagination_and_mixed_order() {
        // Mixed order, multiple entries, ampersands between options.
        let input = "sort[foo]=asc&pagination[page]=2&filter[bar][<=]=10&pagination[size]=50&sort[bar]=desc";
        let (rest, params) =
            parse_cruding_qs_list_filter::<Col>(input, &map_col).expect("parse ok");
        assert_eq!(rest, "");

        // Filters
        assert_eq!(params.filters.len(), 1);
        assert_eq_filter(
            &params.filters[0],
            &Col::Bar,
            &CrudingListFilterOperators::Le(json!(10)),
        );

        // Sorts preserve order of appearance
        assert_eq!(params.sorts.len(), 2);
        assert!(matches!(params.sorts[0].order, CrudingListSortOrder::Asc));
        assert!(matches!(params.sorts[1].order, CrudingListSortOrder::Desc));
        assert!(matches!(params.sorts[0].column, Col::Foo));
        assert!(matches!(params.sorts[1].column, Col::Bar));

        // Pagination
        assert_eq!(params.pagination.page, 2);
        assert_eq!(params.pagination.size, 50);
    }

    #[test]
    fn unknown_column_yields_failure_tag() {
        // "baz" is not mapped by map_col
        let input = "filter[baz][=]=1";
        let res = parse_cruding_qs_list_filter::<Col>(input, &map_col);
        match res {
            Err(nom::Err::Failure(e)) => assert_eq!(e.code, ErrorKind::Tag),
            other => panic!("expected Failure(Tag), got {:?}", other),
        }
    }

    #[test]
    fn in_and_notin_require_arrays() {
        // in with non-array
        let res1 = parse_cruding_qs_list_filter::<Col>("filter[foo][in]=1", &map_col);
        match res1 {
            Err(nom::Err::Failure(e)) => assert_eq!(e.code, ErrorKind::SeparatedList),
            other => panic!("expected Failure(SeparatedList) for in, got {:?}", other),
        }

        // !in with non-array
        let res2 = parse_cruding_qs_list_filter::<Col>("filter[foo][!in]=\"x\"", &map_col);
        match res2 {
            Err(nom::Err::Failure(e)) => assert_eq!(e.code, ErrorKind::SeparatedList),
            other => panic!("expected Failure(SeparatedList) for !in, got {:?}", other),
        }

        // Valid !in
        let (_, ok) =
            parse_cruding_qs_list_filter::<Col>("filter[foo][!in]=[\"a\",\"b\"]", &map_col)
                .expect("parse ok");
        assert_eq!(ok.filters.len(), 1);
        if let CrudingListFilterOperators::NotIn(v) = &ok.filters[0].op {
            assert_eq!(v, &vec![json!("a"), json!("b")]);
        } else {
            panic!("expected NotIn");
        }
    }

    #[test]
    fn accepts_trailing_eof_and_requires_ampersands_between_options() {
        // OK: no trailing ampersand
        let (_, _) =
            parse_cruding_qs_list_filter::<Col>("filter[foo][=]=1", &map_col).expect("ok eof");

        // Missing ampersand between options -> should fail at the `alt((tag(\"&\"), eof))` boundary.
        let bad = "filter[foo][=]=1sort[foo]=asc";
        assert!(parse_cruding_qs_list_filter::<Col>(bad, &map_col).is_err());
    }

    #[test]
    fn parse_filter_operator_unit_checks() {
        // Direct unit checks on the private function (child module can access)
        let (_, op) = parse_filter_operator("[=]=123").expect("ok");
        assert!(matches!(
            op,
            CrudingListFilterOperators::Eq(Value::Number(_))
        ));

        let (_, op) = parse_filter_operator("[>=]=true").expect("ok");
        assert!(matches!(
            op,
            CrudingListFilterOperators::Ge(Value::Bool(true))
        ));

        let res = parse_filter_operator("[in]=42"); // not array
        match res {
            Err(nom::Err::Failure(e)) => assert_eq!(e.code, ErrorKind::SeparatedList),
            other => panic!("expected Failure(SeparatedList), got {:?}", other),
        }
    }
}
