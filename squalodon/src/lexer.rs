use std::{num::NonZeroUsize, str::Chars};

#[derive(Debug, thiserror::Error)]
pub enum LexerError {
    #[error("Unknown token {0:?}")]
    UnknownToken(String),

    #[error("Unexpected end of file")]
    UnexpectedEof,
}

type LexerResult<T> = std::result::Result<T, LexerError>;

macro_rules! keywords {
    ($($v:ident)*) => {
        pub(crate) const KEYWORDS: &[&str] = &[$(stringify!($v)),*];

        #[derive(Clone, PartialEq)]
        pub(crate) enum Token {
            $($v,)*
            Percent,
            LeftParen,
            RightParen,
            Asterisk,
            Plus,
            Comma,
            Minus,
            Dot,
            Slash,
            Semicolon,
            Lt,
            Eq,
            Gt,
            Ne,
            Le,
            Ge,
            PipePipe,
            IntegerLiteral(i64),
            RealLiteral(f64),
            String(String),
            Identifier(String),
            Parameter(NonZeroUsize),
            Comment(String),
            Whitespace(char),
            Eof,
        }

        impl Token {
            fn parse_keyword(s: &str) -> Option<Self> {
                match () {
                    $(_ if s.eq_ignore_ascii_case(stringify!($v)) => Some(Self::$v),)*
                    _ => None,
                }
            }

            pub fn is_literal(&self) -> bool {
                matches!(self, Self::IntegerLiteral(_) | Self::RealLiteral(_) | Self::String(_))
            }

            pub fn is_keyword(&self) -> bool {
                matches!(self, $(Self::$v)|*)
            }
        }

        impl std::fmt::Debug for Token {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(Self::$v => f.write_str(&stringify!($v).to_ascii_uppercase()),)*
                    Self::Percent => f.write_str("%"),
                    Self::LeftParen => f.write_str("("),
                    Self::RightParen => f.write_str(")"),
                    Self::Asterisk => f.write_str("*"),
                    Self::Plus => f.write_str("+"),
                    Self::Comma => f.write_str(","),
                    Self::Minus => f.write_str("-"),
                    Self::Dot => f.write_str("."),
                    Self::Slash => f.write_str("/"),
                    Self::Semicolon => f.write_str(";"),
                    Self::Lt => f.write_str("<"),
                    Self::Eq => f.write_str("="),
                    Self::Gt => f.write_str(">"),
                    Self::Ne => f.write_str("<>"),
                    Self::Le => f.write_str("<="),
                    Self::Ge => f.write_str(">="),
                    Self::PipePipe => f.write_str("||"),
                    Self::IntegerLiteral(i) => i.fmt(f),
                    Self::RealLiteral(r) => r.fmt(f),
                    Self::String(s) => s.fmt(f),
                    Self::Identifier(i) => i.fmt(f),
                    Self::Parameter(p) => write!(f, "${}", p),
                    Self::Comment(c) => write!(f, "--{}", c),
                    Self::Whitespace(c) => c.fmt(f),
                    Self::Eof => f.write_str("EOF"),
                }
            }
        }
    }
}

keywords! {
    All
    And
    As
    Asc
    Begin
    Boolean
    By
    Cast
    Commit
    Create
    Cross
    Deallocate
    Delete
    Desc
    Describe
    Distinct
    Drop
    Execute
    Exists
    Explain
    False
    First
    From
    Group
    Having
    If
    Inner
    Insert
    Integer
    Into
    Join
    Key
    Last
    Limit
    Not
    Null
    Nulls
    Offset
    On
    Or
    Order
    Prepare
    Primary
    Real
    Rollback
    Select
    Set
    Show
    Table
    Tables
    Text
    Transaction
    True
    Update
    Values
    Where
}

pub(crate) struct Lexer<'a> {
    inner: Inner<'a>,
    peeked: Option<Token>,
}

impl<'a> Lexer<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self {
            inner: Inner::new(sql),
            peeked: None,
        }
    }

    pub fn consume(&mut self) -> LexerResult<Token> {
        if let Some(peeked) = self.peeked.take() {
            return Ok(peeked);
        }
        loop {
            match self.inner.consume_token()? {
                Token::Comment(_) | Token::Whitespace(_) => continue,
                token => return Ok(token),
            }
        }
    }

    pub fn consume_if<F>(&mut self, f: F) -> LexerResult<Option<Token>>
    where
        F: Fn(&Token) -> bool,
    {
        Ok(if f(self.peek()?) {
            Some(self.consume()?)
        } else {
            None
        })
    }

    pub fn consume_if_eq(&mut self, token: Token) -> LexerResult<bool> {
        Ok(self.consume_if(|t| *t == token)?.is_some())
    }

    pub fn peek(&mut self) -> LexerResult<&Token> {
        if self.peeked.is_none() {
            self.peeked = Some(self.consume()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }
}

/// An iterator that yields segments of SQL.
///
/// When encountering an error, the iterator will yield the error and
/// the remaining part of the input string.
pub struct Segmenter<'a> {
    sql: &'a str,
    inner: Inner<'a>,
}

impl<'a> Segmenter<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql,
            inner: Inner::new(sql),
        }
    }
}

impl<'a> Iterator for Segmenter<'a> {
    type Item = Result<Segment<'a>, (LexerError, &'a str)>;

    fn next(&mut self) -> Option<Self::Item> {
        let prev_pos = self.inner.pos();
        let token = match self.inner.consume_token() {
            Ok(token) => token,
            Err(e) => return Some(Err((e, &self.sql[prev_pos..]))),
        };
        let kind = match token {
            token if token.is_literal() => SegmentKind::Literal,
            token if token.is_keyword() => SegmentKind::Keyword,
            Token::Comment(_) => SegmentKind::Comment,
            Token::Identifier(_) => SegmentKind::Identifier,
            Token::Whitespace(_) => SegmentKind::Whitespace,
            Token::Eof => return None,
            _ => SegmentKind::Operator,
        };
        Some(Ok(Segment {
            slice: &self.sql[prev_pos..self.inner.pos()],
            kind,
        }))
    }
}

pub struct Segment<'a> {
    slice: &'a str,
    kind: SegmentKind,
}

impl Segment<'_> {
    pub fn slice(&self) -> &str {
        self.slice
    }

    pub fn kind(&self) -> SegmentKind {
        self.kind
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentKind {
    Literal,
    Keyword,
    Comment,
    Identifier,
    Operator,
    Whitespace,
}

struct Inner<'a> {
    chars: Chars<'a>,
    peeked: Option<char>,
    pos: usize,
}

impl<'a> Inner<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            chars: s.chars(),
            peeked: None,
            pos: 0,
        }
    }

    fn pos(&self) -> usize {
        self.pos
    }

    fn consume_token(&mut self) -> LexerResult<Token> {
        let Some(ch) = self.peek() else {
            return Ok(Token::Eof);
        };
        match ch {
            _ if ch.is_control() | ch.is_whitespace() => {
                Ok(Token::Whitespace(self.consume().unwrap()))
            }
            _ if ch.is_ascii_digit() => {
                let literal = self.consume_while(|ch| ch.is_ascii_digit() || ch == '.');
                literal
                    .parse()
                    .map(Token::IntegerLiteral)
                    .or_else(|_| literal.parse().map(Token::RealLiteral))
                    .map_err(|_| LexerError::UnknownToken(literal))
            }
            '\'' => {
                let s = self.consume_string('\'')?;
                Ok(Token::String(s))
            }
            '"' => {
                let ident = self.consume_string('"')?;
                Ok(Token::Identifier(ident))
            }
            _ if ch.is_alphabetic() => {
                let mut s = self.consume_while(is_valid_identifier_char);
                Ok(Token::parse_keyword(&s).unwrap_or_else(|| {
                    s.make_ascii_lowercase();
                    Token::Identifier(s)
                }))
            }
            _ => Ok(if self.consume_if_eq('$') {
                let id_str = self.consume_while(|ch| ch.is_ascii_digit());
                let id = id_str.parse().map_err(|_| {
                    let mut s = "$".to_owned();
                    s.push_str(&id_str);
                    LexerError::UnknownToken(s)
                })?;
                Token::Parameter(id)
            } else if self.consume_if_eq('%') {
                Token::Percent
            } else if self.consume_if_eq('(') {
                Token::LeftParen
            } else if self.consume_if_eq(')') {
                Token::RightParen
            } else if self.consume_if_eq('*') {
                Token::Asterisk
            } else if self.consume_if_eq('+') {
                Token::Plus
            } else if self.consume_if_eq(',') {
                Token::Comma
            } else if self.consume_if_eq('-') {
                if self.consume_if_eq('-') {
                    Token::Comment(self.consume_while(|ch| ch != '\n'))
                } else {
                    Token::Minus
                }
            } else if self.consume_if_eq('.') {
                Token::Dot
            } else if self.consume_if_eq('/') {
                Token::Slash
            } else if self.consume_if_eq(';') {
                Token::Semicolon
            } else if self.consume_if_eq('<') {
                if self.consume_if_eq('>') {
                    Token::Ne
                } else if self.consume_if_eq('=') {
                    Token::Le
                } else {
                    Token::Lt
                }
            } else if self.consume_if_eq('=') {
                Token::Eq
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
                    return Err(LexerError::UnknownToken("!".to_owned()));
                }
            } else if self.consume_if_eq('|') {
                if self.consume_if_eq('|') {
                    Token::PipePipe
                } else {
                    return Err(LexerError::UnknownToken("|".to_owned()));
                }
            } else {
                return Err(LexerError::UnknownToken(ch.to_string()));
            }),
        }
    }

    fn consume_string(&mut self, quote: char) -> LexerResult<String> {
        let ch = self.consume().unwrap();
        assert_eq!(ch, quote);
        let mut s = String::new();
        while let Some(ch) = self.consume() {
            if ch != quote {
                s.push(ch);
                continue;
            }
            if self.consume_if_eq(quote) {
                s.push(quote);
            } else {
                return Ok(s);
            }
        }
        Err(LexerError::UnexpectedEof)
    }

    fn consume(&mut self) -> Option<char> {
        let ch = self.peeked.take().or_else(|| self.chars.next());
        if ch.is_some() {
            self.pos += 1;
        }
        ch
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

    fn consume_while<F>(&mut self, f: F) -> String
    where
        F: Fn(char) -> bool,
    {
        let mut s = String::new();
        while let Some(ch) = self.consume_if(&f) {
            s.push(ch);
        }
        s
    }

    fn peek(&mut self) -> Option<char> {
        if self.peeked.is_none() {
            self.peeked = self.chars.next();
        }
        self.peeked
    }
}

pub fn is_valid_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '$' || ch == '_'
}

pub fn quote(s: &str, quote: char) -> String {
    let mut quoted = String::new();
    quoted.push(quote);
    for ch in s.chars() {
        if ch == quote {
            quoted.push(quote);
        }
        quoted.push(ch);
    }
    quoted.push(quote);
    quoted
}
