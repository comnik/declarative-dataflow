//! Binding language, mainly for use in Hector-powered plans.

use {Aid, Var, Value};

/// A thing that can act as a binding of values to symbols.
pub trait AsBinding {
}

/// Binding types supported by Hector.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Binding {
    /// A symbol bound by (e,v) pairs from an attribute.
    Attribute(AttributeBinding),
    /// A symbol bound by a constant value.
    Constant(ConstantBinding),
}

/// Describes symbols whose possible values are given by an attribute.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AttributeBinding {
    /// The symbols this binding talks about.
    pub symbols: (Var,Var),
    /// The name of a globally known attribute backing this binding.
    pub source_attribute: Aid,
}

/// Describes symbols whose possible values are given by an attribute.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConstantBinding {
    /// The symbol this binding talks about.
    pub symbol: Var,
    /// The value backing this binding.
    pub value: Value,
}
