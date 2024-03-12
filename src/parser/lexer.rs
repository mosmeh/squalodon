use std::str::Chars;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unknown token {0:?}")]
    UnknownToken(String),

    #[error("Unexpected end of file")]
    UnexpectedEof,
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, PartialEq)]
pub enum Token {
    And,
    As,
    Asc,
    Boolean,
    By,
    Create,
    Delete,
    Desc,
    Explain,
    False,
    First,
    From,
    Insert,
    Integer,
    Into,
    Key,
    Last,
    Limit,
    Not,
    Null,
    Nulls,
    Offset,
    Or,
    Order,
    Primary,
    Real,
    Select,
    Set,
    Table,
    Text,
    True,
    Update,
    Values,
    Where,

    Percent,
    LeftParen,
    RightParen,
    Asterisk,
    Plus,
    Comma,
    Minus,
    Slash,
    Semicolon,
    Lt,
    Eq,
    Gt,
    Ne,
    Le,
    Ge,

    IntegerLiteral(i64),
    RealLiteral(f64),
    String(String),
    Identifier(String),

    Eof,
}

impl Token {
    fn parse_keyword(s: &str) -> Option<Self> {
        Some(match s.to_ascii_uppercase().as_str() {
            "AND" => Self::And,
            "AS" => Self::As,
            "ASC" => Self::Asc,
            "BOOLEAN" => Self::Boolean,
            "BY" => Self::By,
            "CREATE" => Self::Create,
            "DELETE" => Self::Delete,
            "DESC" => Self::Desc,
            "EXPLAIN" => Self::Explain,
            "FALSE" => Self::False,
            "FIRST" => Self::First,
            "FROM" => Self::From,
            "INSERT" => Self::Insert,
            "INTEGER" => Self::Integer,
            "INTO" => Self::Into,
            "KEY" => Self::Key,
            "LAST" => Self::Last,
            "LIMIT" => Self::Limit,
            "NOT" => Self::Not,
            "NULL" => Self::Null,
            "NULLS" => Self::Nulls,
            "OFFSET" => Self::Offset,
            "OR" => Self::Or,
            "ORDER" => Self::Order,
            "PRIMARY" => Self::Primary,
            "REAL" => Self::Real,
            "SELECT" => Self::Select,
            "SET" => Self::Set,
            "TABLE" => Self::Table,
            "TEXT" => Self::Text,
            "TRUE" => Self::True,
            "UPDATE" => Self::Update,
            "VALUES" => Self::Values,
            "WHERE" => Self::Where,
            _ => return None,
        })
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::And => f.write_str("AND"),
            Self::As => f.write_str("AS"),
            Self::Asc => f.write_str("ASC"),
            Self::Boolean => f.write_str("BOOLEAN"),
            Self::By => f.write_str("BY"),
            Self::Create => f.write_str("CREATE"),
            Self::Delete => f.write_str("DELETE"),
            Self::Desc => f.write_str("DESC"),
            Self::Explain => f.write_str("EXPLAIN"),
            Self::False => f.write_str("FALSE"),
            Self::First => f.write_str("FIRST"),
            Self::From => f.write_str("FROM"),
            Self::Insert => f.write_str("INSERT"),
            Self::Integer => f.write_str("INTEGER"),
            Self::Into => f.write_str("INTO"),
            Self::Key => f.write_str("KEY"),
            Self::Last => f.write_str("LAST"),
            Self::Limit => f.write_str("LIMIT"),
            Self::Not => f.write_str("NOT"),
            Self::Null => f.write_str("NULL"),
            Self::Nulls => f.write_str("NULLS"),
            Self::Offset => f.write_str("OFFSET"),
            Self::Or => f.write_str("OR"),
            Self::Order => f.write_str("ORDER"),
            Self::Primary => f.write_str("PRIMARY"),
            Self::Real => f.write_str("REAL"),
            Self::Select => f.write_str("SELECT"),
            Self::Set => f.write_str("SET"),
            Self::Table => f.write_str("TABLE"),
            Self::Text => f.write_str("TEXT"),
            Self::True => f.write_str("TRUE"),
            Self::Update => f.write_str("UPDATE"),
            Self::Values => f.write_str("VALUES"),
            Self::Where => f.write_str("WHERE"),
            Self::Percent => f.write_str("%"),
            Self::LeftParen => f.write_str("("),
            Self::RightParen => f.write_str(")"),
            Self::Asterisk => f.write_str("*"),
            Self::Plus => f.write_str("+"),
            Self::Comma => f.write_str(","),
            Self::Minus => f.write_str("-"),
            Self::Slash => f.write_str("/"),
            Self::Semicolon => f.write_str(";"),
            Self::Lt => f.write_str("<"),
            Self::Eq => f.write_str("="),
            Self::Gt => f.write_str(">"),
            Self::Ne => f.write_str("<>"),
            Self::Le => f.write_str("<="),
            Self::Ge => f.write_str(">="),
            Self::IntegerLiteral(i) => i.fmt(f),
            Self::RealLiteral(r) => r.fmt(f),
            Self::String(s) => s.fmt(f),
            Self::Identifier(i) => i.fmt(f),
            Self::Eof => f.write_str("EOF"),
        }
    }
}

pub struct Lexer<'a> {
    inner: LexerInner<'a>,
    peeked: Option<Token>,
}

impl<'a> Lexer<'a> {
    pub fn new(s: &'a str) -> Self {
        Self {
            inner: LexerInner::new(s),
            peeked: None,
        }
    }

    pub fn consume(&mut self) -> Result<Token> {
        if let Some(peeked) = self.peeked.take() {
            Ok(peeked)
        } else {
            self.inner.consume_token()
        }
    }

    pub fn consume_if<F>(&mut self, f: F) -> Result<Option<Token>>
    where
        F: Fn(&Token) -> bool,
    {
        Ok(if f(self.peek()?) {
            Some(self.consume()?)
        } else {
            None
        })
    }

    pub fn consume_if_eq(&mut self, token: Token) -> Result<bool> {
        Ok(self.consume_if(|t| *t == token)?.is_some())
    }

    pub fn peek(&mut self) -> Result<&Token> {
        if self.peeked.is_none() {
            self.peeked = Some(self.consume()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }
}

struct LexerInner<'a> {
    chars: Chars<'a>,
    peeked: Option<char>,
}

impl<'a> LexerInner<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            chars: s.chars(),
            peeked: None,
        }
    }

    fn consume_token(&mut self) -> Result<Token> {
        while let Some(ch) = self.peek() {
            match ch {
                _ if ch.is_control() | ch.is_whitespace() => {
                    self.consume().unwrap();
                }
                _ if ch.is_ascii_digit() => {
                    let mut buf = String::new();
                    self.consume_while(|ch| ch.is_ascii_digit() || ch == '.', &mut buf);
                    return buf
                        .parse()
                        .map(Token::IntegerLiteral)
                        .or_else(|_| buf.parse().map(Token::RealLiteral))
                        .map_err(|_| Error::UnknownToken(buf));
                }
                '\'' => {
                    self.consume().unwrap();
                    let mut buf = String::new();
                    self.consume_while(|ch| ch != '\'', &mut buf);
                    return if self.consume_if_eq('\'') {
                        Ok(Token::String(buf))
                    } else {
                        Err(Error::UnexpectedEof)
                    };
                }
                '"' => {
                    self.consume().unwrap();
                    let mut buf = String::new();
                    self.consume_while(|ch| ch != '"', &mut buf);
                    return if self.consume_if_eq('"') {
                        Ok(Token::Identifier(buf))
                    } else {
                        Err(Error::UnexpectedEof)
                    };
                }
                _ if ch.is_alphabetic() => {
                    let mut buf = String::new();
                    self.consume_while(char::is_alphanumeric, &mut buf);
                    return Ok(Token::parse_keyword(&buf).unwrap_or(Token::Identifier(buf)));
                }
                _ => {
                    return Ok(if self.consume_if_eq('*') {
                        Token::Asterisk
                    } else if self.consume_if_eq(',') {
                        Token::Comma
                    } else if self.consume_if_eq('(') {
                        Token::LeftParen
                    } else if self.consume_if_eq(')') {
                        Token::RightParen
                    } else if self.consume_if_eq('+') {
                        Token::Plus
                    } else if self.consume_if_eq('-') {
                        if self.consume_if_eq('-') {
                            // Comment
                            self.consume_while(|ch| ch != '\n', &mut String::new());
                            continue;
                        }
                        Token::Minus
                    } else if self.consume_if_eq('=') {
                        Token::Eq
                    } else if self.consume_if_eq(';') {
                        Token::Semicolon
                    } else if self.consume_if_eq('/') {
                        Token::Slash
                    } else if self.consume_if_eq('%') {
                        Token::Percent
                    } else if self.consume_if_eq('<') {
                        if self.consume_if_eq('>') {
                            Token::Ne
                        } else if self.consume_if_eq('=') {
                            Token::Le
                        } else {
                            Token::Lt
                        }
                    } else if self.consume_if_eq('>') {
                        if self.consume_if_eq('=') {
                            Token::Ge
                        } else {
                            Token::Gt
                        }
                    } else if self.consume_if_eq('!') {
                        if self.consume_if_eq('=') {
                            Token::Ne
                        } else {
                            return Err(Error::UnknownToken("!".to_owned()));
                        }
                    } else {
                        return Err(Error::UnknownToken(ch.to_string()));
                    });
                }
            }
        }
        Ok(Token::Eof)
    }

    fn consume(&mut self) -> Option<char> {
        self.peeked.take().or_else(|| self.chars.next())
    }

    fn consume_if<F>(&mut self, f: F) -> Option<char>
    where
        F: Fn(char) -> bool,
    {
        if let Some(ch) = self.peek() {
            if f(ch) {
                return self.consume();
            }
        }
        None
    }

    fn consume_if_eq(&mut self, ch: char) -> bool {
        self.consume_if(|c| c == ch).is_some()
    }

    fn consume_while<F>(&mut self, f: F, buf: &mut String)
    where
        F: Fn(char) -> bool,
    {
        while let Some(ch) = self.consume_if(&f) {
            buf.push(ch);
        }
    }

    fn peek(&mut self) -> Option<char> {
        if self.peeked.is_none() {
            self.peeked = self.chars.next();
        }
        self.peeked
    }
}
