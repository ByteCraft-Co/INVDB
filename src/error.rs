//! Unified error model for INVDB operations.

/// Result alias that uses the crate-wide [`InvError`] type.
pub type InvResult<T> = Result<T, InvError>;

/// Errors surfaced by the INVDB engine (INV-3, INV-9).
#[derive(Debug)]
pub enum InvError {
    /// I/O failure during the given action.
    Io {
        action: &'static str,
        source: std::io::Error,
    },
    /// File magic header did not match expectations.
    InvalidMagic {
        expected: [u8; 8],
        found: [u8; 8],
    },
    /// File version was outside the supported range.
    InvalidVersion {
        found: u16,
        min: u16,
        max: u16,
    },
    /// Bytes failed integrity validation.
    Corruption {
        context: &'static str,
        details: String,
    },
    /// An arithmetic or range overflow occurred.
    Overflow {
        context: &'static str,
    },
    /// A caller provided an invalid argument.
    InvalidArgument {
        name: &'static str,
        details: String,
    },
    /// Requested feature is not supported yet.
    Unsupported {
        feature: &'static str,
    },
}

impl InvError {
    /// Helper for wrapping std::io::Error.
    pub fn io(action: &'static str, e: std::io::Error) -> Self {
        Self::Io {
            action,
            source: e,
        }
    }

    /// Helper for constructing corruption errors with context.
    pub fn corruption(context: &'static str, details: impl Into<String>) -> Self {
        Self::Corruption {
            context,
            details: details.into(),
        }
    }

    /// Helper for invalid argument errors.
    pub fn invalid_arg(name: &'static str, details: impl Into<String>) -> Self {
        Self::InvalidArgument {
            name,
            details: details.into(),
        }
    }
}

impl std::fmt::Display for InvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvError::Io { action, source } => write!(f, "I/O error during {}: {}", action, source),
            InvError::InvalidMagic { expected, found } => {
                write!(f, "invalid file magic, expected {:?}, found {:?}", expected, found)
            }
            InvError::InvalidVersion { found, min, max } => write!(
                f,
                "unsupported version {}, supported range is {}..={}",
                found, min, max
            ),
            InvError::Corruption { context, details } => {
                write!(f, "corruption detected in {}: {}", context, details)
            }
            InvError::Overflow { context } => write!(f, "overflow: {}", context),
            InvError::InvalidArgument { name, details } => {
                write!(f, "invalid argument {}: {}", name, details)
            }
            InvError::Unsupported { feature } => write!(f, "unsupported feature: {}", feature),
        }
    }
}

impl std::error::Error for InvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            InvError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
