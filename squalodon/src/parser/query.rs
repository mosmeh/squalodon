use super::{unexpected, Expression, Parser, ParserResult};
use crate::{lexer::Token, ParserError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Select {
    pub distinct: Option<Distinct>,
    pub projections: Vec<Projection>,
    pub from: TableRef,
    pub where_clause: Option<Expression>,
    pub group_by: Vec<Expression>,
    pub having: Option<Expression>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

impl std::fmt::Display for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SELECT ")?;
        if let Some(distinct) = &self.distinct {
            distinct.fmt(f)?;
            f.write_str(" ")?;
        }
        for (i, projection) in self.projections.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            projection.fmt(f)?;
        }
        f.write_str(" FROM ")?;
        self.from.fmt(f)?;
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        if !self.group_by.is_empty() {
            f.write_str(" GROUP BY ")?;
            for (i, expr) in self.group_by.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                expr.fmt(f)?;
            }
        }
        if let Some(having) = &self.having {
            write!(f, " HAVING {having}")?;
        }
        if !self.order_by.is_empty() {
            f.write_str(" ORDER BY ")?;
            for (i, order_by) in self.order_by.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                order_by.fmt(f)?;
            }
        }
        if let Some(limit) = &self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        if let Some(offset) = &self.offset {
            write!(f, " OFFSET {offset}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Distinct {
    pub on: Option<Vec<Expression>>,
}

impl std::fmt::Display for Distinct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DISTINCT")?;
        if let Some(on) = &self.on {
            f.write_str(" ON (")?;
            for (i, expr) in on.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                expr.fmt(f)?;
            }
            f.write_str(")")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Projection {
    Wildcard,
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wildcard => f.write_str("*"),
            Self::Expression { expr, alias } => {
                expr.fmt(f)?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableRef {
    BaseTable { name: String },
    Join(Box<Join>),
    Subquery(Box<Select>),
    Function { name: String, args: Vec<Expression> },
    Values(Values),
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BaseTable { name } => f.write_str(name),
            Self::Join(join) => join.fmt(f),
            Self::Subquery(select) => write!(f, "({select})"),
            Self::Function { name, args } => {
                write!(f, "{name}(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    arg.fmt(f)?;
                }
                f.write_str(")")
            }
            Self::Values(values) => write!(f, "({values})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join {
    pub left: TableRef,
    pub right: TableRef,
    pub on: Option<Expression>,
}

impl std::fmt::Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.left.fmt(f)?;
        match &self.on {
            Some(on) => write!(f, " JOIN {} ON {on}", self.right),
            None => write!(f, " CROSS JOIN {}", self.right),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Values {
    pub rows: Vec<Vec<Expression>>,
}

impl std::fmt::Display for Values {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VALUES ")?;
        for (i, row) in self.rows.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_str("(")?;
            for (j, expr) in row.iter().enumerate() {
                if j > 0 {
                    f.write_str(", ")?;
                }
                expr.fmt(f)?;
            }
            f.write_str(")")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderBy {
    pub expr: Expression,
    pub order: Order,
    pub null_order: NullOrder,
}

impl std::fmt::Display for OrderBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.expr.fmt(f)?;
        if self.order != Order::default() {
            write!(f, " {}", self.order)?;
        }
        if self.null_order != NullOrder::default() {
            write!(f, " {}", self.null_order)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Order {
    Asc,
    Desc,
}

impl Default for Order {
    fn default() -> Self {
        Self::Asc
    }
}

impl std::fmt::Display for Order {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullOrder {
    NullsFirst,
    NullsLast,
}

impl Default for NullOrder {
    fn default() -> Self {
        Self::NullsLast
    }
}

impl std::fmt::Display for NullOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NullsFirst => "NULLS FIRST",
            Self::NullsLast => "NULLS LAST",
        })
    }
}

impl Parser<'_> {
    pub fn parse_select(&mut self) -> ParserResult<Select> {
        let mut distinct = None;
        let projections;
        let from;
        let mut where_clause = None;
        let mut group_by = Vec::new();
        let mut having = None;
        match self.lexer.peek()? {
            Token::Select => {
                self.lexer.consume()?;
                if *self.lexer.peek()? == Token::Distinct {
                    distinct = Some(self.parse_distinct()?);
                }
                projections = self.parse_comma_separated(Self::parse_projection)?;
                from = (*self.lexer.peek()? == Token::From)
                    .then(|| self.parse_from())
                    .transpose()?
                    .unwrap_or(TableRef::Values(Values { rows: Vec::new() }));
                if self.lexer.consume_if_eq(Token::Where)? {
                    where_clause = Some(self.parse_expr()?);
                }
                if self.lexer.consume_if_eq(Token::Group)? {
                    self.expect(Token::By)?;
                    group_by = self.parse_comma_separated(Self::parse_expr)?;
                }
                if self.lexer.consume_if_eq(Token::Having)? {
                    having = Some(self.parse_expr()?);
                }
            }
            Token::Values => {
                self.lexer.consume()?;
                let mut num_columns = None;
                let rows = self.parse_comma_separated(|parser| {
                    parser.expect(Token::LeftParen)?;
                    let exprs = parser.parse_comma_separated(Self::parse_expr)?;
                    parser.expect(Token::RightParen)?;
                    match num_columns {
                        None => num_columns = Some(exprs.len()),
                        Some(n) if n != exprs.len() => {
                            return Err(ParserError::ValuesColumnCountMismatch)
                        }
                        _ => (),
                    }
                    Ok(exprs)
                })?;
                projections = vec![Projection::Wildcard];
                from = TableRef::Values(Values { rows });
            }
            token => return Err(unexpected(token)),
        };
        let order_by = (*self.lexer.peek()? == Token::Order)
            .then(|| self.parse_order_by())
            .transpose()?
            .unwrap_or_default();
        let limit = self
            .lexer
            .consume_if_eq(Token::Limit)?
            .then(|| self.parse_expr())
            .transpose()?;
        let offset = self
            .lexer
            .consume_if_eq(Token::Offset)?
            .then(|| self.parse_expr())
            .transpose()?;
        Ok(Select {
            distinct,
            projections,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_distinct(&mut self) -> ParserResult<Distinct> {
        self.expect(Token::Distinct)?;
        let on = self
            .lexer
            .consume_if_eq(Token::On)?
            .then(|| -> ParserResult<_> {
                self.expect(Token::LeftParen)?;
                let exprs = self.parse_comma_separated(Self::parse_expr)?;
                self.expect(Token::RightParen)?;
                Ok(exprs)
            })
            .transpose()?;
        Ok(Distinct { on })
    }

    fn parse_projection(&mut self) -> ParserResult<Projection> {
        if self.lexer.consume_if_eq(Token::Asterisk)? {
            Ok(Projection::Wildcard)
        } else {
            let expr = self.parse_expr()?;
            let alias = match self.lexer.peek()? {
                Token::As => {
                    self.lexer.consume()?;
                    Some(self.expect_identifier()?)
                }
                Token::Identifier(_) => Some(self.expect_identifier()?),
                _ => None,
            };
            Ok(Projection::Expression { expr, alias })
        }
    }

    fn parse_from(&mut self) -> ParserResult<TableRef> {
        self.expect(Token::From)?;
        let mut table_ref = self.parse_table_ref()?;
        while self.lexer.consume_if_eq(Token::Comma)? {
            // Equivalent to CROSS JOIN
            table_ref = TableRef::Join(Box::new(Join {
                left: table_ref,
                right: self.parse_table_ref()?,
                on: None,
            }));
        }
        Ok(table_ref)
    }

    fn parse_table_ref(&mut self) -> ParserResult<TableRef> {
        let mut table_ref = self.parse_table_or_subquery()?;
        loop {
            match self.lexer.peek()? {
                Token::Cross => {
                    self.lexer.consume()?;
                    self.expect(Token::Join)?;
                    table_ref = TableRef::Join(Box::new(Join {
                        left: table_ref,
                        right: self.parse_table_or_subquery()?,
                        on: None,
                    }));
                }
                Token::Inner => {
                    self.lexer.consume()?;
                    // INNER JOIN is the same as JOIN, so we just consume
                    // INNER and parse JOIN in the next iteration.
                }
                Token::Join => {
                    self.lexer.consume()?;
                    let right = self.parse_table_or_subquery()?;
                    self.expect(Token::On)?;
                    let on = self.parse_expr()?;
                    table_ref = TableRef::Join(Box::new(Join {
                        left: table_ref,
                        right,
                        on: Some(on),
                    }));
                }
                _ => return Ok(table_ref),
            }
        }
    }

    fn parse_table_or_subquery(&mut self) -> ParserResult<TableRef> {
        match self.lexer.peek()? {
            Token::Identifier(_) => {
                let name = self.expect_identifier()?;
                if self.lexer.consume_if_eq(Token::LeftParen)? {
                    let args = if self.lexer.consume_if_eq(Token::RightParen)? {
                        Vec::new()
                    } else {
                        let args = self.parse_comma_separated(Self::parse_expr)?;
                        self.expect(Token::RightParen)?;
                        args
                    };
                    Ok(TableRef::Function { name, args })
                } else {
                    Ok(TableRef::BaseTable { name })
                }
            }
            Token::LeftParen => {
                self.lexer.consume()?;
                let inner = match self.lexer.peek()? {
                    Token::Identifier(_) | Token::LeftParen => self.parse_table_or_subquery()?,
                    Token::Select | Token::Values => {
                        TableRef::Subquery(self.parse_select()?.into())
                    }
                    token => return Err(unexpected(token)),
                };
                self.expect(Token::RightParen)?;
                Ok(inner)
            }
            token => Err(unexpected(token)),
        }
    }

    fn parse_order_by(&mut self) -> ParserResult<Vec<OrderBy>> {
        self.expect(Token::Order)?;
        self.expect(Token::By)?;
        self.parse_comma_separated(|parser| {
            let expr = parser.parse_expr()?;

            let order = if parser.lexer.consume_if_eq(Token::Asc)? {
                Order::Asc
            } else if parser.lexer.consume_if_eq(Token::Desc)? {
                Order::Desc
            } else {
                Default::default()
            };

            let null_order = if parser.lexer.consume_if_eq(Token::Nulls)? {
                match parser.lexer.consume()? {
                    Token::First => NullOrder::NullsFirst,
                    Token::Last => NullOrder::NullsLast,
                    token => return Err(unexpected(&token)),
                }
            } else {
                Default::default()
            };

            Ok(OrderBy {
                expr,
                order,
                null_order,
            })
        })
    }
}
