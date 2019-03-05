//! Binding language, mainly for use in Hector-powered plans.

use std::fmt;

use crate::{Aid, Value, Var};

/// A thing that can act as a binding of values to symbols.
pub trait AsBinding {
    /// All variables bound by this binding.
    fn variables(&self) -> Vec<Var>;

    /// Iff the binding has opinions about the given symbol, this will
    /// return the offset, otherwise None.
    fn binds(&self, sym: Var) -> Option<usize>;

    /// Returns an optional variable which must be bound by the prefix
    /// in order for this binding to extend the prefix. If None, then
    /// this binding can never be used to extend the prefix to the
    /// specified symbol (e.g. because it doesn't even bind it).
    fn required_to_extend(&self, prefix: &AsBinding, target: Var) -> Option<Option<Var>>;

    /// Returns an optional variable by which this binding could
    /// extend the given prefix.
    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var>;

    /// Returns true iff the binding is ready to participate in the
    /// extension of a set of prefix symbols to a new symbol.
    fn can_extend(&self, prefix: &AsBinding, target: Var) -> bool {
        self.ready_to_extend(prefix) == Some(target)
    }
}

impl AsBinding for Vec<Var> {
    fn variables(&self) -> Vec<Var> {
        Vec::new()
    }

    fn binds(&self, sym: Var) -> Option<usize> {
        self.iter().position(|&x| sym == x)
    }

    fn ready_to_extend(&self, _prefix: &AsBinding) -> Option<Var> {
        None
    }

    fn required_to_extend(&self, _prefix: &AsBinding, _target: Var) -> Option<Option<Var>> {
        Some(None)
    }
}

/// Binding types supported by Hector.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
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

impl Binding {
    /// Creates an AttributeBinding.
    pub fn attribute(e: Var, name: &str, v: Var) -> Binding {
        Binding::Attribute(AttributeBinding {
            symbols: (e, v),
            source_attribute: name.to_string(),
            default: None,
        })
    }

    /// Creates an AttributeBinding with a default value.
    pub fn optional_attribute(e: Var, name: &str, v: Var, default: Value) -> Binding {
        Binding::Attribute(AttributeBinding {
            symbols: (e, v),
            source_attribute: name.to_string(),
            default: Some(default),
        })
    }

    /// Creates a ConstantBinding.
    pub fn constant(symbol: Var, value: Value) -> Binding {
        Binding::Constant(ConstantBinding { symbol, value })
    }

    /// Creates a BinaryPredicateBinding.
    pub fn binary_predicate(predicate: BinaryPredicate, x: Var, y: Var) -> Binding {
        Binding::BinaryPredicate(BinaryPredicateBinding {
            symbols: (x, y),
            predicate,
        })
    }
}

impl AsBinding for Binding {
    fn variables(&self) -> Vec<Var> {
        match *self {
            Binding::Attribute(ref binding) => binding.variables(),
            Binding::Not(ref binding) => binding.variables(),
            Binding::Constant(ref binding) => binding.variables(),
            Binding::BinaryPredicate(ref binding) => binding.variables(),
        }
    }

    fn binds(&self, sym: Var) -> Option<usize> {
        match *self {
            Binding::Attribute(ref binding) => binding.binds(sym),
            Binding::Not(ref binding) => binding.binds(sym),
            Binding::Constant(ref binding) => binding.binds(sym),
            Binding::BinaryPredicate(ref binding) => binding.binds(sym),
        }
    }

    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var> {
        match *self {
            Binding::Attribute(ref binding) => binding.ready_to_extend(prefix),
            Binding::Not(ref binding) => binding.ready_to_extend(prefix),
            Binding::Constant(ref binding) => binding.ready_to_extend(prefix),
            Binding::BinaryPredicate(ref binding) => binding.ready_to_extend(prefix),
        }
    }

    fn required_to_extend(&self, prefix: &AsBinding, target: Var) -> Option<Option<Var>> {
        match *self {
            Binding::Attribute(ref binding) => binding.required_to_extend(prefix, target),
            Binding::Not(ref binding) => binding.required_to_extend(prefix, target),
            Binding::Constant(ref binding) => binding.required_to_extend(prefix, target),
            Binding::BinaryPredicate(ref binding) => binding.required_to_extend(prefix, target),
        }
    }
}

/// Describes symbols whose possible values are given by an attribute.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct AttributeBinding {
    /// The symbols this binding talks about.
    pub symbols: (Var, Var),
    /// The name of a globally known attribute backing this binding.
    pub source_attribute: Aid,
    /// Default value of this attribute.
    pub default: Option<Value>,
}

impl AsBinding for AttributeBinding {
    fn variables(&self) -> Vec<Var> {
        vec![self.symbols.0, self.symbols.1]
    }

    fn binds(&self, sym: Var) -> Option<usize> {
        if self.symbols.0 == sym {
            Some(0)
        } else if self.symbols.1 == sym {
            Some(1)
        } else {
            None
        }
    }

    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var> {
        if prefix.binds(self.symbols.0).is_some() && prefix.binds(self.symbols.1).is_none() {
            Some(self.symbols.1)
        } else if prefix.binds(self.symbols.1).is_some() && prefix.binds(self.symbols.0).is_none() {
            Some(self.symbols.0)
        } else {
            None
        }
    }

    fn required_to_extend(&self, prefix: &AsBinding, target: Var) -> Option<Option<Var>> {
        match self.binds(target) {
            None => None,
            Some(offset) => {
                // Self binds target at offset.
                if offset == 0 {
                    // Ensure that the prefix doesn't in fact bind _both_ symbols already.
                    assert!(prefix.binds(self.symbols.0).is_none());
                    match prefix.binds(self.symbols.1) {
                        None => Some(Some(self.symbols.1)),
                        Some(_) => Some(None),
                    }
                } else {
                    // Analogously for the reverse case.
                    assert!(prefix.binds(self.symbols.1).is_none());
                    match prefix.binds(self.symbols.0) {
                        None => Some(Some(self.symbols.0)),
                        Some(_) => Some(None),
                    }
                }
            }
        }
    }
}

impl fmt::Debug for AttributeBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[{} {} {}]",
            self.symbols.0, self.source_attribute, self.symbols.1
        )
    }
}

/// Describes symbols whose possible values must not be contained in
/// the specified attribute.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct AntijoinBinding {
    /// The wrapped binding.
    pub binding: Box<Binding>,
}

impl AsBinding for AntijoinBinding {
    fn variables(&self) -> Vec<Var> {
        self.binding.variables()
    }

    fn binds(&self, sym: Var) -> Option<usize> {
        self.binding.binds(sym)
    }

    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var> {
        self.binding.ready_to_extend(prefix)
    }

    fn required_to_extend(&self, prefix: &AsBinding, target: Var) -> Option<Option<Var>> {
        self.binding.required_to_extend(prefix, target)
    }
}

impl fmt::Debug for AntijoinBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Not({:?})", self.binding)
    }
}

/// Describes symbols whose possible values are given by an attribute.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct ConstantBinding {
    /// The symbol this binding talks about.
    pub symbol: Var,
    /// The value backing this binding.
    pub value: Value,
}

impl AsBinding for ConstantBinding {
    fn variables(&self) -> Vec<Var> {
        vec![self.symbol]
    }

    fn binds(&self, sym: Var) -> Option<usize> {
        if self.symbol == sym {
            Some(0)
        } else {
            None
        }
    }

    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var> {
        if prefix.binds(self.symbol).is_none() {
            Some(self.symbol)
        } else {
            None
        }
    }

    fn required_to_extend(&self, prefix: &AsBinding, target: Var) -> Option<Option<Var>> {
        match self.binds(target) {
            None => None,
            Some(_) => match prefix.binds(target) {
                None => Some(Some(self.symbol)),
                Some(_) => Some(None),
            },
        }
    }
}

impl fmt::Debug for ConstantBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Constant({}, {:?})", self.symbol, self.value)
    }
}

/// Built-in binary predicates.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
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
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct BinaryPredicateBinding {
    /// The symbols this binding talks about.
    pub symbols: (Var, Var),
    /// Logical predicate to apply.
    pub predicate: BinaryPredicate,
}

impl AsBinding for BinaryPredicateBinding {
    fn variables(&self) -> Vec<Var> {
        vec![self.symbols.0, self.symbols.1]
    }

    fn binds(&self, sym: Var) -> Option<usize> {
        if self.symbols.0 == sym {
            Some(0)
        } else if self.symbols.1 == sym {
            Some(1)
        } else {
            None
        }
    }

    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var> {
        if prefix.binds(self.symbols.0).is_some() && prefix.binds(self.symbols.1).is_none() {
            Some(self.symbols.1)
        } else if prefix.binds(self.symbols.1).is_some() && prefix.binds(self.symbols.0).is_none() {
            Some(self.symbols.0)
        } else {
            None
        }
    }

    fn required_to_extend(&self, prefix: &AsBinding, target: Var) -> Option<Option<Var>> {
        match self.binds(target) {
            None => None,
            Some(offset) => {
                // Self binds target at offset.
                if offset == 0 {
                    // Ensure that the prefix doesn't in fact bind _both_ symbols already.
                    assert!(prefix.binds(self.symbols.0).is_none());
                    match prefix.binds(self.symbols.1) {
                        None => Some(Some(self.symbols.1)),
                        Some(_) => Some(None),
                    }
                } else {
                    // Analogously for the reverse case.
                    assert!(prefix.binds(self.symbols.1).is_none());
                    match prefix.binds(self.symbols.0) {
                        None => Some(Some(self.symbols.0)),
                        Some(_) => Some(None),
                    }
                }
            }
        }
    }
}

impl fmt::Debug for BinaryPredicateBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "({:?} {} {})",
            self.predicate, self.symbols.0, self.symbols.1
        )
    }
}
