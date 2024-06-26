mod ddl;
mod expression;
mod mutation;
mod query;

pub use ddl::{
    Analyze, Constraint, CreateIndex, CreateSequence, CreateTable, CreateView, DropObject,
    ObjectKind, Reindex,
};
pub use expression::{BinaryOp, ColumnRef, Expression, FunctionArgs, FunctionCall, UnaryOp};
pub use mutation::{Delete, Insert, Update};
pub use query::{
    Distinct, Join, JoinCondition, NullOrder, Order, OrderBy, Projection, Query, QueryBody,
    QueryModifier, Select, TableRef, TableRefKind, Values,
};

use crate::{
    lexer::{Lexer, LexerError, Token},
    Type,
};
use std::num::NonZeroUsize;

#[derive(thiserror::Error, Debug)]
pub enum ParserError {
    #[error("Unexpected token {0}")]
    UnexpectedToken(String),

    #[error("Rows in VALUES must have the same number of columns")]
    ValuesColumnCountMismatch,

    #[error("Lexer error: {0}")]
    Lexer(#[from] LexerError),
}

impl ParserError {
    fn unexpected(token: &Token) -> Self {
        Self::UnexpectedToken(format!("{token:?}"))
    }
}

pub type ParserResult<T> = std::result::Result<T, ParserError>;

#[derive(Debug, Clone)]
pub enum Statement {
    Explain(Box<Statement>),
    Prepare(Prepare),
    Execute(Execute),
    Deallocate(Deallocate),
    Transaction(TransactionControl),
    ShowTables,
    Describe(String),
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    CreateSequence(CreateSequence),
    CreateView(CreateView),
    Drop(DropObject),
    Truncate(Vec<String>),
    Analyze(Analyze),
    Reindex(Reindex),
    Query(Query),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

impl Statement {
    pub fn is_mutation(&self) -> bool {
        matches!(self, Self::Insert(_) | Self::Update(_) | Self::Delete(_))
    }
}

#[derive(Debug, Clone)]
pub struct Prepare {
    pub name: String,
    pub statement: Box<Statement>,
}

#[derive(Debug, Clone)]
pub struct Execute {
    pub name: String,
    pub params: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub enum Deallocate {
    All,
    Name(String),
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionControl {
    Begin,
    Commit,
    Rollback,
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    max_param_id: Option<NonZeroUsize>,
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
        let statement = self
            .expect_one_of(&[Token::Semicolon, Token::Eof])
            .map(|()| statement);
        Some(statement)
    }
}

impl Token {
    /// Returns true if the token can be the first token in a query.
    fn is_query_prefix(&self) -> bool {
        matches!(self, Self::Select | Self::Values | Self::Table)
    }
}

impl<'a> Parser<'a> {
    pub fn new(s: &'a str) -> Self {
        Self {
            lexer: Lexer::new(s),
            max_param_id: None,
        }
    }

    fn parse_statement(&mut self) -> ParserResult<Statement> {
        match self.lexer.peek()? {
            Token::Prepare => self.parse_prepare().map(Statement::Prepare),
            Token::Execute => self.parse_execute().map(Statement::Execute),
            Token::Deallocate => self.parse_deallocate().map(Statement::Deallocate),
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
            Token::Analyze => {
                // To avoid ambiguity with EXPLAIN ANALYZE, we require
                // the ANALYZE statement to be top-level.
                self.parse_analyze().map(Statement::Analyze)
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
            Token::Drop => self.parse_drop().map(Statement::Drop),
            Token::Truncate => self.parse_truncate().map(Statement::Truncate),
            Token::Reindex => self.parse_reindex().map(Statement::Reindex),
            Token::LeftParen => self.parse_query().map(Statement::Query),
            token if token.is_query_prefix() => self.parse_query().map(Statement::Query),
            Token::Insert => self.parse_insert().map(Statement::Insert),
            Token::Update => self.parse_update().map(Statement::Update),
            Token::Delete => self.parse_delete().map(Statement::Delete),
            token => Err(ParserError::unexpected(token)),
        }
    }

    fn parse_prepare(&mut self) -> ParserResult<Prepare> {
        self.expect(Token::Prepare)?;
        let name = self.expect_identifier()?;
        self.expect(Token::As)?;
        let statement = self.parse_statement_inner()?;
        Ok(Prepare {
            name,
            statement: Box::new(statement),
        })
    }

    fn parse_execute(&mut self) -> ParserResult<Execute> {
        self.expect(Token::Execute)?;
        let name = self.expect_identifier()?;
        let params = if self.lexer.consume_if_eq(Token::LeftParen)? {
            let params = self.parse_comma_separated(Self::parse_expr)?;
            self.expect(Token::RightParen)?;
            params
        } else {
            Vec::new()
        };
        Ok(Execute { name, params })
    }

    fn parse_deallocate(&mut self) -> ParserResult<Deallocate> {
        self.expect(Token::Deallocate)?;
        self.lexer.consume_if_eq(Token::Prepare)?;
        Ok(if self.lexer.consume_if_eq(Token::All)? {
            Deallocate::All
        } else {
            Deallocate::Name(self.expect_identifier()?)
        })
    }

    fn parse_type(&mut self) -> ParserResult<Type> {
        let ty = match self.lexer.consume()? {
            Token::Integer | Token::BigInt | Token::Int | Token::SmallInt => Type::Integer,
            Token::Real | Token::Decimal => Type::Real,
            Token::Boolean => Type::Boolean,
            Token::Text => Type::Text,
            Token::BpChar | Token::Char | Token::VarChar => {
                if self.lexer.consume_if_eq(Token::LeftParen)? {
                    // Length limit is ignored.
                    match self.lexer.peek()? {
                        Token::IntegerLiteral(_) => self.lexer.consume()?,
                        token => return Err(ParserError::unexpected(token)),
                    };
                    self.expect(Token::RightParen)?;
                }
                Type::Text
            }
            Token::Blob | Token::ByteA => Type::Blob,
            token => return Err(ParserError::unexpected(&token)),
        };
        Ok(ty)
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

    fn parse_signed_integer_literal(&mut self) -> ParserResult<i64> {
        let is_negative = if self.lexer.consume_if_eq(Token::Minus)? {
            true
        } else {
            self.lexer.consume_if_eq(Token::Plus)?;
            false
        };
        match self.lexer.consume()? {
            Token::IntegerLiteral(i) if is_negative => Ok(-i),
            Token::IntegerLiteral(i) => Ok(i),
            token => Err(ParserError::unexpected(&token)),
        }
    }

    fn expect(&mut self, expected: Token) -> ParserResult<()> {
        self.expect_one_of(&[expected])
    }

    fn expect_one_of(&mut self, expected: &[Token]) -> ParserResult<()> {
        let actual = self.lexer.consume()?;
        if !expected.contains(&actual) {
            return Err(ParserError::unexpected(&actual));
        }
        Ok(())
    }

    fn expect_identifier(&mut self) -> ParserResult<String> {
        match self.lexer.consume()? {
            Token::Identifier(ident) => Ok(ident),
            token => Err(ParserError::unexpected(&token)),
        }
    }
}
