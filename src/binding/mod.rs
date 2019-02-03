//! Binding language, mainly for use in Hector-powered plans.

use crate::{Aid, Value, Var};

/// A thing that can act as a binding of values to symbols.
pub trait AsBinding {
    /// Iff the binding has opinions about the given symbol, this will
    /// return the offset, otherwise None.
    fn binds(&self, sym: Var) -> Option<usize>;
}

impl AsBinding for Vec<Var> {
    fn binds(&self, sym: Var) -> Option<usize> {
        self.iter().position(|&x| sym == x)
    }
}

/// Binding types supported by Hector.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Binding {
    /// Two symbols bound by (e,v) pairs from an attribute.
    Attribute(AttributeBinding),
    /// Symbols that must not be bound by the wrapped binding.
    Not(AntijoinBinding),
    /// A symbol bound by a constant value.
    Constant(ConstantBinding),
    /// Two symbols bound by a binary predicate.
    BinaryPredicate(BinaryPredicateBinding),
}

impl AsBinding for Binding {
    fn binds(&self, sym: Var) -> Option<usize> {
        match *self {
            Binding::Attribute(ref binding) => binding.binds(sym),
            Binding::Not(ref binding) => binding.binds(sym),
            Binding::Constant(ref binding) => binding.binds(sym),
            Binding::BinaryPredicate(ref binding) => binding.binds(sym),
        }
    }
}

/// Describes symbols whose possible values are given by an attribute.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AttributeBinding {
    /// The symbols this binding talks about.
    pub symbols: (Var, Var),
    /// The name of a globally known attribute backing this binding.
    pub source_attribute: Aid,
}

impl AsBinding for AttributeBinding {
    fn binds(&self, sym: Var) -> Option<usize> {
        if self.symbols.0 == sym {
            Some(0)
        } else if self.symbols.1 == sym {
            Some(1)
        } else {
            None
        }
    }
}

/// Describes symbols whose possible values must not be contained in
/// the specified attribute.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AntijoinBinding {
    /// The wrapped binding.
    pub binding: Box<Binding>,
}

impl AsBinding for AntijoinBinding {
    fn binds(&self, sym: Var) -> Option<usize> {
        self.binding.binds(sym)
    }
}

/// Describes symbols whose possible values are given by an attribute.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConstantBinding {
    /// The symbol this binding talks about.
    pub symbol: Var,
    /// The value backing this binding.
    pub value: Value,
}

impl AsBinding for ConstantBinding {
    fn binds(&self, sym: Var) -> Option<usize> {
        if self.symbol == sym {
            Some(0)
        } else {
            None
        }
    }
}

/// Built-in binary predicates.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum BinaryPredicate {
    /// Less than
    LT,
    /// Greater than
    GT,
    /// Less than or equal to
    LTE,
    /// Greater than or equal to
    GTE,
    /// Equal
    EQ,
    /// Not equal
    NEQ,
}

/// Describe a binary predicate constraint.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BinaryPredicateBinding {
    /// The symbols this binding talks about.
    pub symbols: (Var, Var),
    /// Logical predicate to apply.
    pub predicate: BinaryPredicate,
}

impl AsBinding for BinaryPredicateBinding {
    fn binds(&self, sym: Var) -> Option<usize> {
        if self.symbols.0 == sym {
            Some(0)
        } else if self.symbols.1 == sym {
            Some(1)
        } else {
            None
        }
    }
}
