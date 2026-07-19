use scrape_core::{
    Soup, Tag,
    query::{ScrapeSelector, matches_selector_with_caches},
};
use selectors::{context::SelectorCaches, parser::SelectorList};

use super::reference::{Branch, Compound, Constraint, Reference, Relation};
use crate::selector;

const EPSILON: f64 = 1e-9;

#[derive(Clone, Copy, Default)]
struct Points {
    earned: f64,
    total: f64,
}

impl Points {
    fn add(&mut self, other: Self) {
        self.earned += other.earned;
        self.total += other.total;
    }

    fn score(self) -> f64 {
        if self.total == 0.0 {
            0.0
        } else {
            self.earned / self.total
        }
    }
}

pub(super) fn select<'a>(
    soup: &'a Soup,
    reference: &Reference,
    min: f64,
) -> Result<Vec<Tag<'a>>, selector::Error> {
    let candidates = soup
        .select("*")
        .map_err(|error| selector::Error::Css(error.to_string()))?;
    let mut scored = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let mut candidate_score = 0.0_f64;
        for branch in &reference.branches {
            candidate_score = candidate_score.max(evaluate_branch(candidate, branch));
        }
        scored.push((candidate, candidate_score));
    }
    let highest_score = scored
        .iter()
        .map(|(_, score)| *score)
        .fold(0.0_f64, f64::max);
    if highest_score + EPSILON < min || highest_score == 0.0 {
        return Ok(Vec::new());
    }
    Ok(scored
        .into_iter()
        .filter(|(_, score)| (score - highest_score).abs() <= EPSILON)
        .map(|(candidate, _)| candidate)
        .collect())
}

fn evaluate_branch(candidate: Tag<'_>, branch: &Branch) -> f64 {
    chain(candidate, branch, 0).score()
}

fn chain(candidate: Tag<'_>, branch: &Branch, index: usize) -> Points {
    let mut points = compound(candidate, &branch.compounds[index]);
    let Some(relation) = branch.relations.get(index) else {
        return points;
    };
    let related = related(candidate, *relation);
    points.total += 1.0;
    if related.is_empty() {
        points.total += remaining_total(branch, index + 1);
        return points;
    }
    points.earned += 1.0;
    let mut best = None;
    for node in related {
        let scored = chain(node, branch, index + 1);
        if best.is_none_or(|best: Points| scored.score() > best.score()) {
            best = Some(scored);
        }
    }
    points.add(best.expect("related nodes are not empty"));
    points
}

fn remaining_total(branch: &Branch, index: usize) -> f64 {
    branch.compounds[index..]
        .iter()
        .map(|compound| compound.constraints.len() as f64)
        .sum::<f64>()
        + branch
            .relations
            .get(index..)
            .map_or(0.0, |relations| relations.len() as f64)
}

fn compound(candidate: Tag<'_>, compound: &Compound) -> Points {
    let mut points = Points::default();
    for constraint in &compound.constraints {
        points.total += 1.0;
        points.earned += evaluate_constraint(candidate, constraint);
    }
    points
}

fn evaluate_constraint(candidate: Tag<'_>, constraint: &Constraint) -> f64 {
    match constraint {
        Constraint::Tag(expected) => {
            if candidate.name() == Some(expected.as_str()) {
                1.0
            } else {
                0.0
            }
        }
        Constraint::Id(expected) => similarity(expected, candidate.get("id").unwrap_or_default()),
        Constraint::Class(expected) => candidate
            .classes()
            .map(|class| similarity(expected, class))
            .fold(0.0_f64, f64::max),
        Constraint::Attribute {
            name,
            operation,
            exact,
        } => {
            let Some(value) = candidate.get(name) else {
                return 0.0;
            };
            if matches(candidate, exact.selector_list()) {
                return 1.0;
            }
            match operation {
                None => 1.0,
                Some((_, expected)) => similarity(expected, value),
            }
        }
        Constraint::Not(selectors) => {
            if matches(candidate, selectors) {
                0.0
            } else {
                1.0
            }
        }
        Constraint::Exact(selector) => {
            if matches(candidate, selector.selector_list()) {
                1.0
            } else {
                0.0
            }
        }
    }
}

fn matches(candidate: Tag<'_>, selector: &SelectorList<ScrapeSelector>) -> bool {
    // selectors caches are tied to one SelectorList and cannot be shared across
    // the independently compiled constraints in a healing reference.
    let mut caches = SelectorCaches::default();
    matches_selector_with_caches(
        candidate.document(),
        candidate.node_id(),
        selector,
        &mut caches,
    )
}

fn related<'a>(candidate: Tag<'a>, relation: Relation) -> Vec<Tag<'a>> {
    match relation {
        Relation::Parent => candidate.parent().into_iter().collect(),
        Relation::Ancestor => candidate.parents().collect(),
        Relation::Previous => candidate.prev_sibling().into_iter().collect(),
        Relation::Earlier => candidate.prev_siblings().collect(),
    }
}

fn similarity(expected: &str, actual: &str) -> f64 {
    if expected == actual {
        1.0
    } else if expected.is_empty() || actual.is_empty() {
        0.0
    } else {
        strsim::normalized_levenshtein(expected, actual)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selector::css::healing::reference::Reference;

    fn score(html: &str, expr: &str, target: &str) -> f64 {
        let soup = Soup::parse(html);
        let selector = scrape_core::CompiledSelector::compile(expr).unwrap();
        let reference = Reference::new(&selector).unwrap();
        let candidate = soup.select(target).unwrap()[0];
        reference
            .branches
            .iter()
            .map(|branch| evaluate_branch(candidate, branch))
            .fold(0.0_f64, f64::max)
    }

    #[test]
    fn scores_tag_class_attribute_and_parent_structure() {
        let value = score(
            "<article class='products'><h2 class='titles' data-kind='names'>X</h2></article>",
            "article.product > h2.title[data-kind='name']",
            "h2",
        );
        assert!(value > 0.8);
    }

    #[test]
    fn scores_descendant_and_sibling_relations() {
        assert!(
            score(
                "<main><div><h2 class='titles'>X</h2></div></main>",
                "main h2.title",
                "h2"
            ) > 0.8
        );
        assert!(
            score(
                "<h1>Title</h1><p class='summaries'>Text</p>",
                "h1 + p.summary",
                "p"
            ) > 0.8
        );
    }

    #[test]
    fn negation_is_an_exact_constraint() {
        let allowed = score(
            "<div class='cards disabled-new'></div>",
            "div.card:not(.disabled)",
            "div",
        );
        let rejected = score(
            "<div class='cards disabled'></div>",
            "div.card:not(.disabled)",
            "div",
        );

        assert!(allowed > 0.8);
        assert!(rejected < 0.8);
    }

    #[test]
    fn zero_score_related_nodes_still_contribute_their_total() {
        let value = score(
            "<div><h2 class='title'>X</h2></div>",
            "section.target > h2.title",
            "h2",
        );

        assert!(value < 0.8);
    }
}
