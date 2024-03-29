mod lexer;

pub use lexer::{quote, LexerError};

use crate::{
    catalog::Column,
    types::{Type, Value},
};
use lexer::{Lexer, Token};

#[derive(thiserror::Error, Debug)]
pub enum ParserError {
    #[error("Unexpected token {0:?}")]
    UnexpectedToken(Token),

    #[error("Rows in VALUES must have the same number of columns")]
    ValuesColumnCountMismatch,

    #[error("Lexer error: {0}")]
    Lexer(#[from] LexerError),
}

pub type ParserResult<T> = std::result::Result<T, ParserError>;

#[derive(Debug)]
pub enum Statement {
    Explain(Box<Statement>),
    Transaction(TransactionControl),
    ShowTables,
    Describe(String),
    CreateTable(CreateTable),
    DropTable(DropTable),
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionControl {
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug)]
pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Constraint {
    PrimaryKey(Vec<String>),
    NotNull(String),
}

#[derive(Debug)]
pub struct DropTable {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug)]
pub struct Select {
    pub projections: Vec<Projection>,
    pub from: TableRef,
    pub where_clause: Option<Expression>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

#[derive(Debug)]
pub enum Projection {
    Wildcard,
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

#[derive(Debug)]
pub enum TableRef {
    BaseTable { name: String },
    Join(Box<Join>),
    Subquery(Box<Select>),
    Function { name: String, args: Vec<Expression> },
    Values(Values),
}

#[derive(Debug)]
pub struct Join {
    pub left: TableRef,
    pub right: TableRef,
    pub on: Option<Expression>,
}

#[derive(Debug)]
pub struct Values {
    pub rows: Vec<Vec<Expression>>,
}

#[derive(Debug)]
pub struct OrderBy {
    pub expr: Expression,
    pub order: Order,
    pub null_order: NullOrder,
}

#[derive(Debug)]
pub struct Insert {
    pub table_name: String,
    pub column_names: Option<Vec<String>>,
    pub select: Select,
}

#[derive(Debug)]
pub struct Update {
    pub table_name: String,
    pub sets: Vec<Set>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug)]
pub struct Set {
    pub column_name: String,
    pub expr: Expression,
}

#[derive(Debug)]
pub struct Delete {
    pub table_name: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub enum Expression {
    Constant(Value),
    ColumnRef(ColumnRef),
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    BinaryOp {
        op: BinaryOp,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
}

#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub table_name: Option<String>,
    pub column_name: String,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Plus,
    Minus,
    Not,
}

impl std::fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Not => "NOT",
        })
    }
}

impl UnaryOp {
    fn from_token(token: &Token) -> Option<Self> {
        Some(match token {
            Token::Plus => Self::Plus,
            Token::Minus => Self::Minus,
            Token::Not => Self::Not,
            _ => return None,
        })
    }

    fn priority(self) -> usize {
        // https://www.sqlite.org/lang_expr.html#operators_and_parse_affecting_attributes
        match self {
            Self::Plus | Self::Minus => 9,
            Self::Not => 3,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Concat,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Concat => "||",
        })
    }
}

impl BinaryOp {
    fn from_token(token: &Token) -> Option<Self> {
        Some(match token {
            Token::And => Self::And,
            Token::Or => Self::Or,
            Token::Percent => Self::Mod,
            Token::Asterisk => Self::Mul,
            Token::Plus => Self::Add,
            Token::Minus => Self::Sub,
            Token::Slash => Self::Div,
            Token::Lt => Self::Lt,
            Token::Eq => Self::Eq,
            Token::Gt => Self::Gt,
            Token::Ne => Self::Ne,
            Token::Le => Self::Le,
            Token::Ge => Self::Ge,
            Token::PipePipe => Self::Concat,
            _ => return None,
        })
    }

    fn priority(self) -> usize {
        // https://www.sqlite.org/lang_expr.html#operators_and_parse_affecting_attributes
        match self {
            Self::Concat => 8,
            Self::Mul | Self::Div | Self::Mod => 7,
            Self::Add | Self::Sub => 6,
            Self::Lt | Self::Le | Self::Gt | Self::Ge => 5,
            Self::Eq | Self::Ne => 4,
            Self::And => 2,
            Self::Or => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

pub struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl Iterator for Parser<'_> {
    type Item = ParserResult<Statement>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.lexer.peek() {
                Ok(Token::Eof) => return None,
                Ok(Token::Semicolon) => {
                    self.lexer.consume().unwrap();
                }
                Ok(_) => break,
                Err(e) => return Some(Err(e.into())),
            }
        }
        let statement = match self.parse_statement() {
            Ok(statement) => statement,
            Err(e) => return Some(Err(e)),
        };
        Some(match self.expect_one_of(&[Token::Semicolon, Token::Eof]) {
            Ok(()) => Ok(statement),
            Err(e) => Err(e),
        })
    }
}

impl<'a> Parser<'a> {
    pub fn new(s: &'a str) -> Self {
        Self {
            lexer: Lexer::new(s),
        }
    }

    fn parse_statement(&mut self) -> ParserResult<Statement> {
        match self.lexer.peek()? {
            Token::Begin => {
                self.lexer.consume()?;
                self.lexer.consume_if_eq(Token::Transaction)?;
                Ok(Statement::Transaction(TransactionControl::Begin))
            }
            Token::Commit => {
                self.lexer.consume()?;
                self.lexer.consume_if_eq(Token::Transaction)?;
                Ok(Statement::Transaction(TransactionControl::Commit))
            }
            Token::Rollback => {
                self.lexer.consume()?;
                self.lexer.consume_if_eq(Token::Transaction)?;
                Ok(Statement::Transaction(TransactionControl::Rollback))
            }
            _ => self.parse_statement_inner(),
        }
    }

    fn parse_statement_inner(&mut self) -> ParserResult<Statement> {
        match self.lexer.peek()? {
            Token::Explain => {
                self.lexer.consume()?;
                Ok(Statement::Explain(Box::new(self.parse_statement_inner()?)))
            }
            Token::Show => {
                self.lexer.consume()?;
                self.expect(Token::Tables)?;
                Ok(Statement::ShowTables)
            }
            Token::Describe => {
                self.lexer.consume()?;
                let name = self.expect_identifier()?;
                Ok(Statement::Describe(name))
            }
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Select | Token::Values => self.parse_select().map(Statement::Select),
            Token::Insert => self.parse_insert().map(Statement::Insert),
            Token::Update => self.parse_update().map(Statement::Update),
            Token::Delete => self.parse_delete().map(Statement::Delete),
            token => Err(ParserError::UnexpectedToken(token.clone())),
        }
    }

    fn parse_create(&mut self) -> ParserResult<Statement> {
        self.expect(Token::Create)?;
        match self.lexer.peek()? {
            Token::Table => self.parse_create_table().map(Statement::CreateTable),
            token => Err(ParserError::UnexpectedToken(token.clone())),
        }
    }

    fn parse_create_table(&mut self) -> ParserResult<CreateTable> {
        self.expect(Token::Table)?;
        let if_not_exists = self
            .lexer
            .consume_if_eq(Token::If)?
            .then(|| {
                self.expect(Token::Not)?;
                self.expect(Token::Exists)
            })
            .transpose()?
            .is_some();
        let name = self.expect_identifier()?;
        self.expect(Token::LeftParen)?;
        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        loop {
            match self.lexer.peek()? {
                Token::Primary => {
                    self.lexer.consume()?;
                    self.expect(Token::Key)?;
                    self.expect(Token::LeftParen)?;
                    constraints.push(Constraint::PrimaryKey(
                        self.parse_comma_separated(Self::expect_identifier)?,
                    ));
                    self.expect(Token::RightParen)?;
                }
                Token::Identifier(_) => {
                    let name = self.expect_identifier()?;
                    let ty = match self.lexer.consume()? {
                        Token::Integer => Type::Integer,
                        Token::Real => Type::Real,
                        Token::Boolean => Type::Boolean,
                        Token::Text => Type::Text,
                        token => return Err(ParserError::UnexpectedToken(token)),
                    };
                    loop {
                        match self.lexer.peek()? {
                            Token::Primary => {
                                self.lexer.consume()?;
                                self.expect(Token::Key)?;
                                constraints.push(Constraint::PrimaryKey(vec![name.clone()]));
                            }
                            Token::Not => {
                                self.lexer.consume()?;
                                self.expect(Token::Null)?;
                                constraints.push(Constraint::NotNull(name.clone()));
                            }
                            _ => break,
                        }
                    }
                    columns.push(Column { name, ty });
                }
                token => return Err(ParserError::UnexpectedToken(token.clone())),
            }
            if !self.lexer.consume_if_eq(Token::Comma)? {
                break;
            }
        }
        self.expect(Token::RightParen)?;
        Ok(CreateTable {
            name,
            if_not_exists,
            columns,
            constraints,
        })
    }

    fn parse_drop(&mut self) -> ParserResult<Statement> {
        self.expect(Token::Drop)?;
        match self.lexer.peek()? {
            Token::Table => self.parse_drop_table().map(Statement::DropTable),
            token => Err(ParserError::UnexpectedToken(token.clone())),
        }
    }

    fn parse_drop_table(&mut self) -> ParserResult<DropTable> {
        self.expect(Token::Table)?;
        let if_exists = self
            .lexer
            .consume_if_eq(Token::If)?
            .then(|| self.expect(Token::Exists))
            .transpose()?
            .is_some();
        let name = self.expect_identifier()?;
        Ok(DropTable { name, if_exists })
    }

    fn parse_select(&mut self) -> ParserResult<Select> {
        let projections;
        let from;
        let where_clause;
        match self.lexer.peek()? {
            Token::Select => {
                self.lexer.consume()?;
                projections = self.parse_comma_separated(Self::parse_projection)?;
                from = (*self.lexer.peek()? == Token::From)
                    .then(|| self.parse_from())
                    .transpose()?
                    .unwrap_or(TableRef::Values(Values { rows: Vec::new() }));
                where_clause = self
                    .lexer
                    .consume_if_eq(Token::Where)?
                    .then(|| self.parse_expr())
                    .transpose()?;
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
                where_clause = None;
            }
            token => return Err(ParserError::UnexpectedToken(token.clone())),
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
            projections,
            from,
            where_clause,
            order_by,
            limit,
            offset,
        })
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
                if *self.lexer.peek()? == Token::LeftParen {
                    let args = self.parse_args()?;
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
                    token => return Err(ParserError::UnexpectedToken(token.clone())),
                };
                self.expect(Token::RightParen)?;
                Ok(inner)
            }
            token => Err(ParserError::UnexpectedToken(token.clone())),
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
                    token => return Err(ParserError::UnexpectedToken(token)),
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

    fn parse_insert(&mut self) -> ParserResult<Insert> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        let table_name = self.expect_identifier()?;
        let column_names = self
            .lexer
            .consume_if_eq(Token::LeftParen)?
            .then(|| -> ParserResult<_> {
                let column_names = self.parse_comma_separated(Self::expect_identifier)?;
                self.expect(Token::RightParen)?;
                Ok(column_names)
            })
            .transpose()?;
        let select = self.parse_select()?;
        Ok(Insert {
            table_name,
            column_names,
            select,
        })
    }

    fn parse_update(&mut self) -> ParserResult<Update> {
        self.expect(Token::Update)?;
        let table_name = self.expect_identifier()?;
        self.expect(Token::Set)?;
        let sets = self.parse_comma_separated(Self::parse_set)?;
        let where_clause = self
            .lexer
            .consume_if_eq(Token::Where)?
            .then(|| self.parse_expr())
            .transpose()?;
        Ok(Update {
            table_name,
            sets,
            where_clause,
        })
    }

    fn parse_set(&mut self) -> ParserResult<Set> {
        let column_name = self.expect_identifier()?;
        self.expect(Token::Eq)?;
        let expr = self.parse_expr()?;
        Ok(Set { column_name, expr })
    }

    fn parse_delete(&mut self) -> ParserResult<Delete> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;
        let table_name = self.expect_identifier()?;
        let where_clause = self
            .lexer
            .consume_if_eq(Token::Where)?
            .then(|| self.parse_expr())
            .transpose()?;
        Ok(Delete {
            table_name,
            where_clause,
        })
    }

    fn parse_expr(&mut self) -> ParserResult<Expression> {
        self.parse_sub_expr(0)
    }

    fn parse_sub_expr(&mut self, min_priority: usize) -> ParserResult<Expression> {
        let mut expr = match UnaryOp::from_token(self.lexer.peek()?) {
            Some(op) => {
                self.lexer.consume()?;
                let expr = self.parse_sub_expr(op.priority())?;
                Expression::UnaryOp {
                    op,
                    expr: expr.into(),
                }
            }
            None => self.parse_atom()?,
        };
        loop {
            let Some(op) = BinaryOp::from_token(self.lexer.peek()?) else {
                break;
            };
            let priority = op.priority();
            if priority <= min_priority {
                break;
            }
            self.lexer.consume()?;
            let rhs = self.parse_sub_expr(priority)?;
            expr = Expression::BinaryOp {
                op,
                lhs: expr.into(),
                rhs: rhs.into(),
            };
        }
        Ok(expr)
    }

    fn parse_atom(&mut self) -> ParserResult<Expression> {
        let expr = match self.lexer.consume()? {
            Token::Null => Expression::Constant(Value::Null),
            Token::IntegerLiteral(i) => Expression::Constant(Value::Integer(i)),
            Token::RealLiteral(f) => Expression::Constant(Value::Real(f)),
            Token::True => Expression::Constant(Value::Boolean(true)),
            Token::False => Expression::Constant(Value::Boolean(false)),
            Token::String(s) => Expression::Constant(Value::Text(s)),
            Token::Identifier(ident) => {
                if self.lexer.consume_if_eq(Token::Dot)? {
                    let column = self.expect_identifier()?;
                    Expression::ColumnRef(ColumnRef {
                        table_name: Some(ident),
                        column_name: column,
                    })
                } else {
                    Expression::ColumnRef(ColumnRef {
                        table_name: None,
                        column_name: ident,
                    })
                }
            }
            Token::LeftParen => {
                let expr = self.parse_expr()?;
                self.expect(Token::RightParen)?;
                expr
            }
            token => return Err(ParserError::UnexpectedToken(token)),
        };
        Ok(expr)
    }

    fn parse_comma_separated<T, F>(&mut self, mut f: F) -> ParserResult<Vec<T>>
    where
        F: FnMut(&mut Self) -> ParserResult<T>,
    {
        let mut items = Vec::new();
        loop {
            items.push(f(self)?);
            if !self.lexer.consume_if_eq(Token::Comma)? {
                break;
            }
        }
        Ok(items)
    }

    fn parse_args(&mut self) -> ParserResult<Vec<Expression>> {
        self.expect(Token::LeftParen)?;
        if self.lexer.consume_if_eq(Token::RightParen)? {
            return Ok(Vec::new());
        }
        let args = self.parse_comma_separated(Self::parse_expr)?;
        self.expect(Token::RightParen)?;
        Ok(args)
    }

    fn expect(&mut self, expected: Token) -> ParserResult<()> {
        self.expect_one_of(&[expected])
    }

    fn expect_one_of(&mut self, expected: &[Token]) -> ParserResult<()> {
        let actual = self.lexer.consume()?;
        if !expected.contains(&actual) {
            return Err(ParserError::UnexpectedToken(actual));
        }
        Ok(())
    }

    fn expect_identifier(&mut self) -> ParserResult<String> {
        match self.lexer.consume()? {
            Token::Identifier(ident) => Ok(ident),
            token => Err(ParserError::UnexpectedToken(token)),
        }
    }
}
