use super::{Parser, ParserResult, Query, Statement};
use crate::{catalog::Column, lexer::Token, ParserError};

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Constraint {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub name: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub is_unique: bool,
}

#[derive(Debug, Clone)]
pub struct CreateSequence {
    pub name: String,
    pub if_not_exists: bool,
    pub increment_by: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub start_value: Option<i64>,
    pub cycle: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct CreateView {
    pub name: String,
    pub column_names: Option<Vec<String>>,
    pub query: Box<Query>,
}

#[derive(Debug, Clone)]
pub struct DropObject {
    pub names: Vec<String>,
    pub kind: ObjectKind,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub enum ObjectKind {
    Table,
    Index,
    Sequence,
    View,
}

impl std::fmt::Display for ObjectKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::Table => "TABLE",
            Self::Index => "INDEX",
            Self::Sequence => "SEQUENCE",
            Self::View => "VIEW",
        })
    }
}

#[derive(Debug, Clone)]
pub struct Analyze {
    pub table_names: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub enum Reindex {
    Table(String),
    Index(String),
}

impl Parser<'_> {
    pub fn parse_create(&mut self) -> ParserResult<Statement> {
        self.expect(Token::Create)?;
        match self.lexer.peek()? {
            Token::Table => self.parse_create_table().map(Statement::CreateTable),
            Token::Index | Token::Unique => self.parse_create_index().map(Statement::CreateIndex),
            Token::Sequence => self.parse_create_sequence().map(Statement::CreateSequence),
            Token::View => self.parse_create_view().map(Statement::CreateView),
            token => Err(ParserError::unexpected(token)),
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
                    let column_names = self.parse_comma_separated(Self::expect_identifier)?;
                    self.expect(Token::RightParen)?;
                    constraints.push(Constraint::PrimaryKey(column_names));
                }
                Token::Unique => {
                    self.lexer.consume()?;
                    self.expect(Token::LeftParen)?;
                    let column_names = self.parse_comma_separated(Self::expect_identifier)?;
                    self.expect(Token::RightParen)?;
                    constraints.push(Constraint::Unique(column_names));
                }
                Token::Identifier(_) => {
                    let name = self.expect_identifier()?;
                    let ty = self.parse_type()?;
                    let mut is_nullable = true;
                    let mut default_value = None;
                    loop {
                        match self.lexer.peek()? {
                            Token::Primary => {
                                self.lexer.consume()?;
                                self.expect(Token::Key)?;
                                constraints.push(Constraint::PrimaryKey(vec![name.clone()]));
                            }
                            Token::Null => {
                                self.lexer.consume()?;
                                // Nullable by default
                            }
                            Token::Not => {
                                self.lexer.consume()?;
                                self.expect(Token::Null)?;
                                is_nullable = false;
                            }
                            Token::Unique => {
                                self.lexer.consume()?;
                                constraints.push(Constraint::Unique(vec![name.clone()]));
                            }
                            Token::Default => {
                                self.lexer.consume()?;
                                default_value = Some(self.parse_expr()?);
                            }
                            _ => break,
                        }
                    }
                    columns.push(Column {
                        name,
                        ty,
                        is_nullable,
                        default_value,
                    });
                }
                token => return Err(ParserError::unexpected(token)),
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

    fn parse_create_index(&mut self) -> ParserResult<CreateIndex> {
        let is_unique = self.lexer.consume_if_eq(Token::Unique)?;
        self.expect(Token::Index)?;
        let name = self.expect_identifier()?;
        self.expect(Token::On)?;
        let table_name = self.expect_identifier()?;
        self.expect(Token::LeftParen)?;
        let column_names = self.parse_comma_separated(Self::expect_identifier)?;
        self.expect(Token::RightParen)?;
        Ok(CreateIndex {
            name,
            table_name,
            column_names,
            is_unique,
        })
    }

    fn parse_create_sequence(&mut self) -> ParserResult<CreateSequence> {
        self.expect(Token::Sequence)?;
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
        let mut increment_by = None;
        let mut min_value = None;
        let mut max_value = None;
        let mut start_value = None;
        let mut cycle = None;
        loop {
            match self.lexer.peek()? {
                Token::Increment if increment_by.is_none() => {
                    self.lexer.consume()?;
                    self.lexer.consume_if_eq(Token::By)?;
                    increment_by = Some(self.parse_signed_integer_literal()?);
                }
                Token::MinValue if min_value.is_none() => {
                    self.lexer.consume()?;
                    min_value = Some(self.parse_signed_integer_literal()?);
                }
                Token::MaxValue if max_value.is_none() => {
                    self.lexer.consume()?;
                    max_value = Some(self.parse_signed_integer_literal()?);
                }
                Token::Start if start_value.is_none() => {
                    self.lexer.consume()?;
                    self.lexer.consume_if_eq(Token::With)?;
                    start_value = Some(self.parse_signed_integer_literal()?);
                }
                Token::Cycle if cycle.is_none() => {
                    self.lexer.consume()?;
                    cycle = Some(true);
                }
                Token::No => {
                    self.lexer.consume()?;
                    match self.lexer.peek()? {
                        Token::MinValue => {
                            self.lexer.consume()?;
                            min_value = None;
                        }
                        Token::MaxValue => {
                            self.lexer.consume()?;
                            max_value = None;
                        }
                        Token::Cycle if cycle.is_none() => {
                            self.lexer.consume()?;
                            cycle = Some(false);
                        }
                        token => return Err(ParserError::unexpected(token)),
                    }
                }
                _ => break,
            }
        }
        Ok(CreateSequence {
            name,
            if_not_exists,
            increment_by,
            min_value,
            max_value,
            start_value,
            cycle,
        })
    }

    fn parse_create_view(&mut self) -> ParserResult<CreateView> {
        self.expect(Token::View)?;
        let name = self.expect_identifier()?;
        let column_names = self
            .lexer
            .consume_if_eq(Token::LeftParen)?
            .then(|| -> ParserResult<_> {
                let column_names = self.parse_comma_separated(Self::expect_identifier)?;
                self.expect(Token::RightParen)?;
                Ok(column_names)
            })
            .transpose()?;
        self.expect(Token::As)?;
        let query = self.parse_query()?;
        Ok(CreateView {
            name,
            column_names,
            query: Box::new(query),
        })
    }

    pub fn parse_drop(&mut self) -> ParserResult<DropObject> {
        self.expect(Token::Drop)?;
        let kind = if self.lexer.consume_if_eq(Token::Table)? {
            ObjectKind::Table
        } else if self.lexer.consume_if_eq(Token::Index)? {
            ObjectKind::Index
        } else if self.lexer.consume_if_eq(Token::Sequence)? {
            ObjectKind::Sequence
        } else if self.lexer.consume_if_eq(Token::View)? {
            ObjectKind::View
        } else {
            return Err(ParserError::unexpected(self.lexer.peek()?));
        };
        let if_exists = self
            .lexer
            .consume_if_eq(Token::If)?
            .then(|| self.expect(Token::Exists))
            .transpose()?
            .is_some();
        let names = self.parse_comma_separated(Self::expect_identifier)?;
        Ok(DropObject {
            names,
            kind,
            if_exists,
        })
    }

    pub fn parse_truncate(&mut self) -> ParserResult<Vec<String>> {
        self.expect(Token::Truncate)?;
        self.lexer.consume_if_eq(Token::Table)?;
        self.parse_comma_separated(Self::expect_identifier)
    }

    pub fn parse_analyze(&mut self) -> ParserResult<Analyze> {
        self.expect(Token::Analyze)?;
        let table_names = matches!(self.lexer.peek()?, Token::Identifier(_))
            .then(|| self.parse_comma_separated(Self::expect_identifier))
            .transpose()?;
        Ok(Analyze { table_names })
    }

    pub fn parse_reindex(&mut self) -> ParserResult<Reindex> {
        self.expect(Token::Reindex)?;
        if self.lexer.consume_if_eq(Token::Table)? {
            Ok(Reindex::Table(self.expect_identifier()?))
        } else if self.lexer.consume_if_eq(Token::Index)? {
            Ok(Reindex::Index(self.expect_identifier()?))
        } else {
            Err(ParserError::unexpected(self.lexer.peek()?))
        }
    }
}
