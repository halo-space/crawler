use std::collections::HashMap;

use scrape_core::{
    NodeId, Soup, Tag,
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

#[derive(Default)]
struct Cache {
    points: HashMap<(NodeId, usize), Points>,
    ancestors: HashMap<(NodeId, usize), Option<Points>>,
    earlier: HashMap<(NodeId, usize), Option<Points>>,
    #[cfg(test)]
    calls: usize,
    #[cfg(test)]
    relations: usize,
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
    let mut caches = reference
        .branches
        .iter()
        .map(|_| Cache::default())
        .collect::<Vec<_>>();
    for candidate in candidates {
        let mut candidate_score = 0.0_f64;
        for (branch, cache) in reference.branches.iter().zip(&mut caches) {
            candidate_score = candidate_score.max(evaluate_branch(candidate, branch, cache));
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

fn evaluate_branch(candidate: Tag<'_>, branch: &Branch, cache: &mut Cache) -> f64 {
    chain(candidate, branch, 0, cache).score()
}

fn chain(candidate: Tag<'_>, branch: &Branch, index: usize, cache: &mut Cache) -> Points {
    #[cfg(test)]
    {
        cache.calls += 1;
    }
    let key = (candidate.node_id(), index);
    if let Some(points) = cache.points.get(&key) {
        return *points;
    }

    let mut points = compound(candidate, &branch.compounds[index]);
    let Some(relation) = branch.relations.get(index) else {
        cache.points.insert(key, points);
        return points;
    };
    let related = match relation {
        Relation::Parent => candidate
            .parent()
            .map(|node| chain(node, branch, index + 1, cache)),
        Relation::Ancestor => ancestor(candidate, branch, index, cache),
        Relation::Previous => candidate
            .prev_sibling()
            .map(|node| chain(node, branch, index + 1, cache)),
        Relation::Earlier => earlier(candidate, branch, index, cache),
    };
    points.total += 1.0;
    let Some(related) = related else {
        points.total += remaining_total(branch, index + 1);
        cache.points.insert(key, points);
        return points;
    };
    points.earned += 1.0;
    points.add(related);
    cache.points.insert(key, points);
    points
}

fn ancestor(
    candidate: Tag<'_>,
    branch: &Branch,
    index: usize,
    cache: &mut Cache,
) -> Option<Points> {
    let key = (candidate.node_id(), index);
    if let Some(points) = cache.ancestors.get(&key) {
        return *points;
    }

    let mut pending = Vec::new();
    let mut current = candidate;
    let tail = loop {
        let key = (current.node_id(), index);
        if let Some(points) = cache.ancestors.get(&key) {
            break *points;
        }
        #[cfg(test)]
        {
            cache.relations += 1;
        }
        pending.push(current);
        let Some(parent) = current.parent() else {
            break None;
        };
        current = parent;
    };

    let mut points = tail;
    for node in pending.into_iter().rev() {
        let value = node.parent().map(|parent| {
            let nearest = chain(parent, branch, index + 1, cache);
            prefer(nearest, points)
        });
        cache.ancestors.insert((node.node_id(), index), value);
        points = value;
    }
    points
}

fn earlier(candidate: Tag<'_>, branch: &Branch, index: usize, cache: &mut Cache) -> Option<Points> {
    let key = (candidate.node_id(), index);
    if let Some(points) = cache.earlier.get(&key) {
        return *points;
    }

    let mut pending = Vec::new();
    let mut current = candidate;
    let tail = loop {
        let key = (current.node_id(), index);
        if let Some(points) = cache.earlier.get(&key) {
            break *points;
        }
        #[cfg(test)]
        {
            cache.relations += 1;
        }
        pending.push(current);
        let Some(previous) = current.prev_sibling() else {
            break None;
        };
        current = previous;
    };

    let mut points = tail;
    for node in pending.into_iter().rev() {
        let value = node.prev_sibling().map(|previous| {
            let nearest = chain(previous, branch, index + 1, cache);
            prefer(nearest, points)
        });
        cache.earlier.insert((node.node_id(), index), value);
        points = value;
    }
    points
}

fn prefer(nearest: Points, previous: Option<Points>) -> Points {
    match previous {
        Some(previous) if previous.score() > nearest.score() => previous,
        _ => nearest,
    }
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
            .map(|branch| evaluate_branch(candidate, branch, &mut Cache::default()))
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

    #[test]
    fn memoizes_deep_descendant_and_sibling_scoring_states() {
        const DEPTH: usize = 28;
        const COMPOUNDS: usize = 10;

        let descendants = format!(
            "{}target{}",
            "<section class='near'>".repeat(DEPTH),
            "</section>".repeat(DEPTH)
        );
        bounded_calls(&descendants, &["section.target"; COMPOUNDS].join(" "));

        let siblings = "<section class='near'></section>".repeat(DEPTH);
        bounded_calls(&siblings, &["section.target"; COMPOUNDS].join(" ~ "));
    }

    #[test]
    fn handles_thousands_of_ancestors_and_earlier_siblings_without_recursion() {
        const ELEMENTS: usize = 4_096;

        let descendants = format!(
            "{}target{}",
            "<section class='near'>".repeat(ELEMENTS),
            "</section>".repeat(ELEMENTS)
        );
        let soup = Soup::parse_with_config(
            &descendants,
            scrape_core::SoupConfig::builder()
                .max_depth(ELEMENTS + 16)
                .build(),
        );
        bounded_calls_in(soup, "main section.target");

        let siblings = "<section class='near'></section>".repeat(ELEMENTS);
        bounded_calls(&siblings, "main ~ section.target");
    }

    fn bounded_calls(html: &str, expr: &str) {
        bounded_calls_in(Soup::parse(html), expr);
    }

    fn bounded_calls_in(soup: Soup, expr: &str) {
        let selector = scrape_core::CompiledSelector::compile(expr).unwrap();
        let reference = Reference::new(&selector).unwrap();
        let branch = &reference.branches[0];
        let candidate = *soup.select("section").unwrap().last().unwrap();
        let elements = soup.select("*").unwrap().len();
        let compounds = branch.compounds.len();
        let mut cache = Cache::default();

        let result = chain(candidate, branch, 0, &mut cache);

        assert!(result.total > 0.0);
        assert!(cache.points.len() <= elements * compounds);
        assert!(cache.ancestors.len() + cache.earlier.len() <= elements * compounds);
        assert!(cache.relations <= elements * compounds);
        assert!(cache.calls <= elements * compounds * 3);
    }
}
