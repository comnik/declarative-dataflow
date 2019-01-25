//! Binding language, mainly for use in Hector-powered plans.

use {Aid, Var, Value};

/// A thing that can act as a binding of values to symbols.
pub trait AsBinding {
    /// Iff the binding has opinions about the given symbol, this will
    /// return the offset, otherwise None.
    fn binds(&self, sym: &Var) -> Option<usize>;
}

/// Binding types supported by Hector.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Binding {
    /// A symbol bound by (e,v) pairs from an attribute.
    Attribute(AttributeBinding),
    /// A symbol bound by a constant value.
    Constant(ConstantBinding),
}

impl AsBinding for Binding {
    fn binds(&self, sym: &Var) -> Option<usize> {
        match self {
            &Binding::Attribute(ref binding) => binding.binds(sym),
            &Binding::Constant(ref binding) => binding.binds(sym),
        }
    }
}

/// Describes symbols whose possible values are given by an attribute.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AttributeBinding {
    /// The symbols this binding talks about.
    pub symbols: (Var,Var),
    /// The name of a globally known attribute backing this binding.
    pub source_attribute: Aid,
}

impl AsBinding for AttributeBinding {
    fn binds(&self, sym: &Var) -> Option<usize> {
        if self.symbols.0 == *sym { Some(0) }
        else if self.symbols.1 == *sym { Some(1) }
        else { None }
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
    fn binds(&self, sym: &Var) -> Option<usize> {
        if self.symbol == *sym { Some(0) }
        else { None }
    }
}
