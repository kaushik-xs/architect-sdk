//! RSQL filter and sort parser.
//!
//! Syntax: `field<op>value` joined by `;` (AND) or `,` (OR), grouped with `()`.
//!
//! Operators: `==` `!=` `=gt=` `=ge=` `=lt=` `=le=` `=in=` `=out=`
//!            `=like=` `=ilike=` `=contains=` `=starts=` `=ends=`
//!            `=between=` `=null=`
//!
//! Sort: `?sort=-created_at,name`  (`-` prefix = descending)

use crate::AppError;

// ─── Public types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum RsqlOp {
    Eq,
    Neq,
    Gt,
    Ge,
    Lt,
    Le,
    In,
    Out,
    Like,
    Ilike,
    Contains,
    Starts,
    Ends,
    Between,
    /// `=null=true` → IS NULL; `=null=false` → IS NOT NULL
    Null(bool),
}

impl RsqlOp {
    pub fn display(&self) -> &'static str {
        match self {
            RsqlOp::Eq => "==",
            RsqlOp::Neq => "!=",
            RsqlOp::Gt => "=gt=",
            RsqlOp::Ge => "=ge=",
            RsqlOp::Lt => "=lt=",
            RsqlOp::Le => "=le=",
            RsqlOp::In => "=in=",
            RsqlOp::Out => "=out=",
            RsqlOp::Like => "=like=",
            RsqlOp::Ilike => "=ilike=",
            RsqlOp::Contains => "=contains=",
            RsqlOp::Starts => "=starts=",
            RsqlOp::Ends => "=ends=",
            RsqlOp::Between => "=between=",
            RsqlOp::Null(_) => "=null=",
        }
    }
}

#[derive(Debug, Clone)]
pub enum FilterNode {
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    Leaf {
        field: String,
        op: RsqlOp,
        /// Parsed string values (empty for Null; one for scalar ops; ≥1 for In/Out/Between).
        values: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub struct SortSpec {
    pub field: String,
    pub desc: bool,
}

// ─── Public entry points ───────────────────────────────────────────────────────

/// Parse an RSQL expression string into a `FilterNode` tree.
/// Returns `AppError::Validation` (HTTP 422) on any parse error.
pub fn parse_rsql(input: &str) -> Result<FilterNode, AppError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation("empty RSQL expression".into()));
    }
    let mut p = Parser::new(trimmed);
    let node = p
        .parse_expression()
        .map_err(|e| AppError::Validation(format!("RSQL parse error: {}", e)))?;
    if !p.at_end() {
        return Err(AppError::Validation(format!(
            "RSQL parse error: unexpected token at position {}",
            p.pos
        )));
    }
    Ok(node)
}

/// Parse a `sort` query parameter into a list of `SortSpec`.
/// Format: `field1,-field2,field3`  (`-` prefix = descending).
/// Silently skips empty tokens.
pub fn parse_sort(input: &str) -> Vec<SortSpec> {
    input
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            if let Some(field) = part.strip_prefix('-') {
                if field.is_empty() {
                    return None;
                }
                Some(SortSpec { field: field.to_string(), desc: true })
            } else {
                Some(SortSpec { field: part.to_string(), desc: false })
            }
        })
        .collect()
}

// ─── Parser ───────────────────────────────────────────────────────────────────

struct Parser {
    chars: Vec<char>,
    pub pos: usize,
}

impl Parser {
    fn new(input: &str) -> Self {
        Parser { chars: input.chars().collect(), pos: 0 }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn at_end(&self) -> bool {
        self.pos >= self.chars.len()
    }

    // expression = or_expr
    fn parse_expression(&mut self) -> Result<FilterNode, String> {
        self.parse_or()
    }

    // or_expr = and_expr (',' and_expr)*
    // NOTE: ',' inside =in=(...) / =out=(...) / =between=(...) is consumed by
    // parse_arguments before we return here, so no ambiguity.
    fn parse_or(&mut self) -> Result<FilterNode, String> {
        let first = self.parse_and()?;
        let mut parts = vec![first];
        while self.peek() == Some(',') {
            self.pos += 1;
            parts.push(self.parse_and()?);
        }
        if parts.len() == 1 { Ok(parts.remove(0)) } else { Ok(FilterNode::Or(parts)) }
    }

    // and_expr = atom (';' atom)*
    fn parse_and(&mut self) -> Result<FilterNode, String> {
        let first = self.parse_atom()?;
        let mut parts = vec![first];
        while self.peek() == Some(';') {
            self.pos += 1;
            parts.push(self.parse_atom()?);
        }
        if parts.len() == 1 { Ok(parts.remove(0)) } else { Ok(FilterNode::And(parts)) }
    }

    // atom = '(' expression ')' | leaf
    fn parse_atom(&mut self) -> Result<FilterNode, String> {
        if self.peek() == Some('(') {
            self.pos += 1;
            let node = self.parse_expression()?;
            if self.peek() != Some(')') {
                return Err(format!("expected ')' at position {}", self.pos));
            }
            self.pos += 1;
            Ok(node)
        } else {
            self.parse_leaf()
        }
    }

    fn parse_leaf(&mut self) -> Result<FilterNode, String> {
        let field = self.parse_selector()?;
        let (op_raw, op_name) = self.parse_operator()?;

        // Special handling for =null=: parse true/false value and bake into op
        if op_name == "null" {
            let raw = self.parse_value()?;
            let is_null = match raw.to_lowercase().as_str() {
                "true"  => true,
                "false" => false,
                other   => return Err(format!("=null= expects true or false, got '{}'", other)),
            };
            return Ok(FilterNode::Leaf { field, op: RsqlOp::Null(is_null), values: vec![] });
        }

        let values = self.parse_arguments(&op_raw)?;
        Ok(FilterNode::Leaf { field, op: op_raw, values })
    }

    fn parse_selector(&mut self) -> Result<String, String> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_alphanumeric() || c == '_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(format!("expected field name at position {}", self.pos));
        }
        Ok(self.chars[start..self.pos].iter().collect())
    }

    /// Returns (RsqlOp, op_name_string).
    fn parse_operator(&mut self) -> Result<(RsqlOp, String), String> {
        // Two-char operators: == and !=
        let two: String = self.chars.get(self.pos..self.pos + 2).map(|s| s.iter().collect()).unwrap_or_default();
        if two == "==" { self.pos += 2; return Ok((RsqlOp::Eq,  "==".into())); }
        if two == "!=" { self.pos += 2; return Ok((RsqlOp::Neq, "!=".into())); }

        // Named operators: =name=
        if self.peek() == Some('=') {
            self.pos += 1; // skip opening =
            let start = self.pos;
            while let Some(c) = self.peek() {
                if c == '=' { break; }
                self.pos += 1;
            }
            if self.peek() != Some('=') {
                return Err(format!("unterminated operator at position {}", self.pos));
            }
            let name: String = self.chars[start..self.pos].iter().collect();
            self.pos += 1; // skip closing =
            let op = match name.as_str() {
                "gt"       => RsqlOp::Gt,
                "ge"       => RsqlOp::Ge,
                "lt"       => RsqlOp::Lt,
                "le"       => RsqlOp::Le,
                "in"       => RsqlOp::In,
                "out"      => RsqlOp::Out,
                "like"     => RsqlOp::Like,
                "ilike"    => RsqlOp::Ilike,
                "contains" => RsqlOp::Contains,
                "starts"   => RsqlOp::Starts,
                "ends"     => RsqlOp::Ends,
                "between"  => RsqlOp::Between,
                "null"     => RsqlOp::Null(true), // placeholder; fixed in parse_leaf
                _ => return Err(format!("unknown operator '={}=' at position {}", name, self.pos)),
            };
            return Ok((op, name));
        }

        Err(format!("expected operator at position {}", self.pos))
    }

    fn parse_arguments(&mut self, op: &RsqlOp) -> Result<Vec<String>, String> {
        match op {
            RsqlOp::In | RsqlOp::Out | RsqlOp::Between => {
                // Expect (val1,val2,...)
                if self.peek() != Some('(') {
                    return Err(format!("expected '(' after operator at position {}", self.pos));
                }
                self.pos += 1;
                let mut values = Vec::new();
                loop {
                    values.push(self.parse_value()?);
                    match self.peek() {
                        Some(',') => { self.pos += 1; }
                        Some(')') => { self.pos += 1; break; }
                        _ => return Err(format!("expected ',' or ')' at position {}", self.pos)),
                    }
                }
                Ok(values)
            }
            _ => Ok(vec![self.parse_value()?]),
        }
    }

    fn parse_value(&mut self) -> Result<String, String> {
        if self.peek() == Some('"') {
            // Quoted value — read until closing unescaped "
            self.pos += 1;
            let mut val = String::new();
            loop {
                match self.peek() {
                    None      => return Err(format!("unterminated quoted value at position {}", self.pos)),
                    Some('"') => { self.pos += 1; break; }
                    Some(c)   => { val.push(c); self.pos += 1; }
                }
            }
            Ok(val)
        } else {
            // Unquoted — read until , ; ( ) or end
            let start = self.pos;
            while let Some(c) = self.peek() {
                if ",;()".contains(c) { break; }
                self.pos += 1;
            }
            Ok(self.chars[start..self.pos].iter().collect())
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_eq() {
        let node = parse_rsql("status==active").unwrap();
        assert!(matches!(node, FilterNode::Leaf { op: RsqlOp::Eq, .. }));
    }

    #[test]
    fn test_and() {
        let node = parse_rsql("status==active;age=ge=18").unwrap();
        assert!(matches!(node, FilterNode::And(_)));
    }

    #[test]
    fn test_or() {
        let node = parse_rsql("status==active,status==pending").unwrap();
        assert!(matches!(node, FilterNode::Or(_)));
    }

    #[test]
    fn test_in_does_not_split_on_comma() {
        // The , inside (active,pending) must NOT be treated as OR
        let node = parse_rsql("status=in=(active,pending);age=gt=0").unwrap();
        assert!(matches!(node, FilterNode::And(_)));
        if let FilterNode::And(ref parts) = node {
            assert_eq!(parts.len(), 2);
            if let FilterNode::Leaf { op: RsqlOp::In, values, .. } = &parts[0] {
                assert_eq!(values, &["active", "pending"]);
            } else {
                panic!("expected In leaf");
            }
        }
    }

    #[test]
    fn test_null_true() {
        let node = parse_rsql("deleted_at=null=true").unwrap();
        assert!(matches!(node, FilterNode::Leaf { op: RsqlOp::Null(true), .. }));
    }

    #[test]
    fn test_null_false() {
        let node = parse_rsql("email=null=false").unwrap();
        assert!(matches!(node, FilterNode::Leaf { op: RsqlOp::Null(false), .. }));
    }

    #[test]
    fn test_between() {
        let node = parse_rsql("age=between=(18,65)").unwrap();
        if let FilterNode::Leaf { op: RsqlOp::Between, values, .. } = node {
            assert_eq!(values, &["18", "65"]);
        } else {
            panic!("expected Between leaf");
        }
    }

    #[test]
    fn test_grouped_or_inside_and() {
        let node = parse_rsql("status==active;(role==admin,role==moderator)").unwrap();
        assert!(matches!(node, FilterNode::And(_)));
    }

    #[test]
    fn test_quoted_value() {
        let node = parse_rsql(r#"name=="John Doe""#).unwrap();
        if let FilterNode::Leaf { values, .. } = node {
            assert_eq!(values[0], "John Doe");
        }
    }

    #[test]
    fn test_sort_parse() {
        let specs = parse_sort("-created_at,name");
        assert_eq!(specs.len(), 2);
        assert!(specs[0].desc);
        assert_eq!(specs[0].field, "created_at");
        assert!(!specs[1].desc);
        assert_eq!(specs[1].field, "name");
    }

    #[test]
    fn test_unknown_op_errors() {
        assert!(parse_rsql("age=foo=5").is_err());
    }

    #[test]
    fn test_null_bad_value_errors() {
        assert!(parse_rsql("deleted_at=null=yes").is_err());
    }
}
