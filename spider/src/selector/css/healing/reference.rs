use scrape_core::{CompiledSelector, query::ScrapeSelector};
use selectors::attr::{AttrSelectorOperator, ParsedAttrSelectorOperation};
use selectors::parser::{Combinator, Component, RelativeSelectorMatchHint, Selector, SelectorList};

use crate::selector;

pub(super) struct Reference {
    pub branches: Vec<Branch>,
}

pub(super) struct Branch {
    pub compounds: Vec<Compound>,
    pub relations: Vec<Relation>,
}

#[derive(Default)]
pub(super) struct Compound {
    pub constraints: Vec<Constraint>,
}

pub(super) enum Constraint {
    Tag(String),
    Id(String),
    Class(String),
    Attribute {
        name: String,
        operation: Option<(AttrSelectorOperator, String)>,
        exact: CompiledSelector,
    },
    Any(Vec<Branch>),
    Not(SelectorList<ScrapeSelector>),
    Has(Vec<(RelativeSelectorMatchHint, Branch)>),
    Exact(CompiledSelector),
}

#[derive(Clone, Copy)]
pub(super) enum Relation {
    Parent,
    Ancestor,
    Previous,
    Earlier,
}

impl Reference {
    pub fn new(selector: &CompiledSelector) -> Result<Self, selector::Error> {
        Ok(Self {
            branches: branches(selector.selector_list())?,
        })
    }
}

fn branches(list: &SelectorList<ScrapeSelector>) -> Result<Vec<Branch>, selector::Error> {
    list.slice().iter().map(branch).collect()
}

fn branch(selector: &Selector<ScrapeSelector>) -> Result<Branch, selector::Error> {
    let mut compounds = vec![Compound::default()];
    let mut relations = Vec::new();
    for component in selector.iter_raw_match_order() {
        match component {
            Component::Combinator(combinator) => {
                relations.push(relation(*combinator)?);
                compounds.push(Compound::default());
            }
            _ => {
                if let Some(constraint) = constraint(component)? {
                    compounds
                        .last_mut()
                        .expect("compound exists")
                        .constraints
                        .push(constraint);
                }
            }
        }
    }
    Ok(Branch {
        compounds,
        relations,
    })
}

fn relation(combinator: Combinator) -> Result<Relation, selector::Error> {
    match combinator {
        Combinator::Child => Ok(Relation::Parent),
        Combinator::Descendant => Ok(Relation::Ancestor),
        Combinator::NextSibling => Ok(Relation::Previous),
        Combinator::LaterSibling => Ok(Relation::Earlier),
        _ => Err(selector::Error::Css(format!(
            "unsupported healing combinator: {combinator:?}"
        ))),
    }
}

fn constraint(
    component: &Component<ScrapeSelector>,
) -> Result<Option<Constraint>, selector::Error> {
    match component {
        Component::LocalName(name) => {
            Ok(Some(Constraint::Tag(name.lower_name.as_str().to_string())))
        }
        Component::ID(id) => Ok(Some(Constraint::Id(id.as_str().to_string()))),
        Component::Class(class) => Ok(Some(Constraint::Class(class.as_str().to_string()))),
        Component::AttributeInNoNamespaceExists {
            local_name_lower, ..
        } => Ok(Some(attribute(component, local_name_lower.as_str(), None)?)),
        Component::AttributeInNoNamespace {
            local_name,
            operator,
            value,
            ..
        } => Ok(Some(attribute(
            component,
            local_name.as_str(),
            Some((*operator, value.as_str().to_string())),
        )?)),
        Component::AttributeOther(value) => {
            let operation = match &value.operation {
                ParsedAttrSelectorOperation::Exists => None,
                ParsedAttrSelectorOperation::WithValue {
                    operator, value, ..
                } => Some((*operator, value.as_str().to_string())),
            };
            Ok(Some(attribute(
                component,
                value.local_name_lower.as_str(),
                operation,
            )?))
        }
        Component::Is(list) | Component::Where(list) => Ok(Some(Constraint::Any(branches(list)?))),
        Component::Negation(list) => Ok(Some(Constraint::Not(list.clone()))),
        Component::Has(selectors) => Ok(Some(Constraint::Has(
            selectors
                .iter()
                .map(|selector| Ok((selector.match_hint, branch(&selector.selector)?)))
                .collect::<Result<_, selector::Error>>()?,
        ))),
        Component::ExplicitUniversalType
        | Component::ExplicitAnyNamespace
        | Component::ExplicitNoNamespace
        | Component::DefaultNamespace(_)
        | Component::Namespace(_, _)
        | Component::RelativeSelectorAnchor
        | Component::ImplicitScope => Ok(None),
        Component::Invalid(value) => Err(selector::Error::Css(format!(
            "invalid healing selector component: {value}"
        ))),
        other => exact(other).map(Some),
    }
}

fn attribute(
    component: &Component<ScrapeSelector>,
    name: &str,
    operation: Option<(AttrSelectorOperator, String)>,
) -> Result<Constraint, selector::Error> {
    let exact = match exact(component)? {
        Constraint::Exact(selector) => selector,
        _ => unreachable!(),
    };
    Ok(Constraint::Attribute {
        name: name.to_string(),
        operation,
        exact,
    })
}

fn exact(component: &Component<ScrapeSelector>) -> Result<Constraint, selector::Error> {
    let expr = format!("*{component:?}");
    CompiledSelector::compile(&expr)
        .map(Constraint::Exact)
        .map_err(|error| selector::Error::Css(error.to_string()))
}
