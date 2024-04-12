use super::{unexpected, Parser, ParserResult, Statement};
use crate::{catalog::Column, lexer::Token};

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
    NotNull(String),
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
pub struct DropObject {
    pub name: String,
    pub kind: ObjectKind,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub enum ObjectKind {
    Table,
    Index,
}

impl std::fmt::Display for ObjectKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Table => "TABLE",
            Self::Index => "INDEX",
        })
    }
}

impl Parser<'_> {
    pub fn parse_create(&mut self) -> ParserResult<Statement> {
        self.expect(Token::Create)?;
        match self.lexer.peek()? {
            Token::Table => self.parse_create_table().map(Statement::CreateTable),
            Token::Index | Token::Unique => self.parse_create_index().map(Statement::CreateIndex),
            token => Err(unexpected(token)),
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
                                constraints.push(Constraint::NotNull(name.clone()));
                            }
                            Token::Unique => {
                                self.lexer.consume()?;
                                constraints.push(Constraint::Unique(vec![name.clone()]));
                            }
                            _ => break,
                        }
                    }
                    columns.push(Column { name, ty });
                }
                token => return Err(unexpected(token)),
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

    pub fn parse_drop(&mut self) -> ParserResult<DropObject> {
        self.expect(Token::Drop)?;
        let kind = if self.lexer.consume_if_eq(Token::Table)? {
            ObjectKind::Table
        } else if self.lexer.consume_if_eq(Token::Index)? {
            ObjectKind::Index
        } else {
            return Err(unexpected(self.lexer.peek()?));
        };
        let if_exists = self
            .lexer
            .consume_if_eq(Token::If)?
            .then(|| self.expect(Token::Exists))
            .transpose()?
            .is_some();
        let name = self.expect_identifier()?;
        Ok(DropObject {
            name,
            kind,
            if_exists,
        })
    }
}
