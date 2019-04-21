//! Binding language, mainly for use in Hector-powered plans.

use std::fmt;

use crate::{Aid, Value, Var};

/// A thing that can act as a binding of values to variables.
pub trait AsBinding {
    /// All variables bound by this binding.
    fn variables(&self) -> Vec<Var>;

    /// Iff the binding has opinions about the given variable, this will
    /// return the offset, otherwise None.
    fn binds(&self, variable: Var) -> Option<usize>;

    /// Returns an optional variable which must be bound by the prefix
    /// in order for this binding to extend the prefix. If None, then
    /// this binding can never be used to extend the prefix to the
    /// specified variable (e.g. because it doesn't even bind it).
    fn required_to_extend(&self, prefix: &AsBinding, target: Var) -> Option<Option<Var>>;

    /// Returns an optional variable by which this binding could
    /// extend the given prefix.
    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var>;

    /// Returns true iff the binding is ready to participate in the
    /// extension of a set of prefix variables to a new variable.
    fn can_extend(&self, prefix: &AsBinding, target: Var) -> bool {
        self.ready_to_extend(prefix) == Some(target)
    }
}

impl AsBinding for Vec<Var> {
    fn variables(&self) -> Vec<Var> {
        Vec::new()
    }

    fn binds(&self, variable: Var) -> Option<usize> {
        self.iter().position(|&x| variable == x)
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
    /// Two variables bound by (e,v) pairs from an attribute.
    Attribute(AttributeBinding),
    /// Variables that must not be bound by the wrapped binding.
    Not(AntijoinBinding),
    /// A variable bound by a constant value.
    Constant(ConstantBinding),
    /// Two variables bound by a binary predicate.
    BinaryPredicate(BinaryPredicateBinding),
}

impl Binding {
    /// Creates an AttributeBinding.
    pub fn attribute(e: Var, name: &str, v: Var) -> Binding {
        Binding::Attribute(AttributeBinding {
            variables: (e, v),
            source_attribute: name.to_string(),
        })
    }

    /// Creates a ConstantBinding.
    pub fn constant(variable: Var, value: Value) -> Binding {
        Binding::Constant(ConstantBinding { variable, value })
    }

    /// Creates a BinaryPredicateBinding.
    pub fn binary_predicate(predicate: BinaryPredicate, x: Var, y: Var) -> Binding {
        Binding::BinaryPredicate(BinaryPredicateBinding {
            variables: (x, y),
            predicate,
        })
    }

    /// Creates an AntijoinBinding.
    pub fn not(binding: Binding) -> Binding {
        Binding::Not(AntijoinBinding {
            binding: Box::new(binding),
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

    fn binds(&self, variable: Var) -> Option<usize> {
        match *self {
            Binding::Attribute(ref binding) => binding.binds(variable),
            Binding::Not(ref binding) => binding.binds(variable),
            Binding::Constant(ref binding) => binding.binds(variable),
            Binding::BinaryPredicate(ref binding) => binding.binds(variable),
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

/// Describes variables whose possible values are given by an attribute.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct AttributeBinding {
    /// The variables this binding talks about.
    pub variables: (Var, Var),
    /// The name of a globally known attribute backing this binding.
    pub source_attribute: Aid,
}

impl AsBinding for AttributeBinding {
    fn variables(&self) -> Vec<Var> {
        vec![self.variables.0, self.variables.1]
    }

    fn binds(&self, variable: Var) -> Option<usize> {
        if self.variables.0 == variable {
            Some(0)
        } else if self.variables.1 == variable {
            Some(1)
        } else {
            None
        }
    }

    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var> {
        if prefix.binds(self.variables.0).is_some() && prefix.binds(self.variables.1).is_none() {
            Some(self.variables.1)
        } else if prefix.binds(self.variables.1).is_some()
            && prefix.binds(self.variables.0).is_none()
        {
            Some(self.variables.0)
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
                    // Ensure that the prefix doesn't in fact bind _both_ variables already.
                    assert!(prefix.binds(self.variables.0).is_none());
                    match prefix.binds(self.variables.1) {
                        None => Some(Some(self.variables.1)),
                        Some(_) => Some(None),
                    }
                } else {
                    // Analogously for the reverse case.
                    assert!(prefix.binds(self.variables.1).is_none());
                    match prefix.binds(self.variables.0) {
                        None => Some(Some(self.variables.0)),
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
            self.variables.0, self.source_attribute, self.variables.1
        )
    }
}

/// Describes variables whose possible values must not be contained in
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

    fn binds(&self, variable: Var) -> Option<usize> {
        self.binding.binds(variable)
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

/// Describes variables whose possible values are given by an attribute.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct ConstantBinding {
    /// The variable this binding talks about.
    pub variable: Var,
    /// The value backing this binding.
    pub value: Value,
}

impl AsBinding for ConstantBinding {
    fn variables(&self) -> Vec<Var> {
        vec![self.variable]
    }

    fn binds(&self, variable: Var) -> Option<usize> {
        if self.variable == variable {
            Some(0)
        } else {
            None
        }
    }

    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var> {
        if prefix.binds(self.variable).is_none() {
            Some(self.variable)
        } else {
            None
        }
    }

    fn required_to_extend(&self, prefix: &AsBinding, target: Var) -> Option<Option<Var>> {
        match self.binds(target) {
            None => None,
            Some(_) => match prefix.binds(target) {
                None => Some(Some(self.variable)),
                Some(_) => Some(None),
            },
        }
    }
}

impl fmt::Debug for ConstantBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Constant({}, {:?})", self.variable, self.value)
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
    /// The variables this binding talks about.
    pub variables: (Var, Var),
    /// Logical predicate to apply.
    pub predicate: BinaryPredicate,
}

impl AsBinding for BinaryPredicateBinding {
    fn variables(&self) -> Vec<Var> {
        vec![self.variables.0, self.variables.1]
    }

    fn binds(&self, variable: Var) -> Option<usize> {
        if self.variables.0 == variable {
            Some(0)
        } else if self.variables.1 == variable {
            Some(1)
        } else {
            None
        }
    }

    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var> {
        if prefix.binds(self.variables.0).is_some() && prefix.binds(self.variables.1).is_none() {
            Some(self.variables.1)
        } else if prefix.binds(self.variables.1).is_some()
            && prefix.binds(self.variables.0).is_none()
        {
            Some(self.variables.0)
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
                    // Ensure that the prefix doesn't in fact bind _both_ variables already.
                    assert!(prefix.binds(self.variables.0).is_none());
                    match prefix.binds(self.variables.1) {
                        None => Some(Some(self.variables.1)),
                        Some(_) => Some(None),
                    }
                } else {
                    // Analogously for the reverse case.
                    assert!(prefix.binds(self.variables.1).is_none());
                    match prefix.binds(self.variables.0) {
                        None => Some(Some(self.variables.0)),
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
            self.predicate, self.variables.0, self.variables.1
        )
    }
}
