"""One vocabulary, one place (PROTOCOL §2.2, §0.2).

Before this module the type names lived in four places that disagreed: the
protocol table, the plugin's JSON-schema enum, the server's write path (which
accepted any string) and the corpus. The result was measurable — `prior-art`
and `spec` were declared in §0.2 and could not be written by any client, and
`type: "Claim"` passed the falsifier gate because every check keyed on the
exact lowercase string.

Rules:
  * canonical names only, lowercase, hyphenated;
  * aliases normalise instead of being rejected (`open-question` → `question`);
  * an unknown name is a 400, not a new type: a typo used to create a node that
    no rule could ever see.
"""

CANONICAL = (
    "claim", "anomaly", "question", "decision", "task", "review",
    "link", "tombstone", "suggestion",
    # §0.2 project-tier names. They are legal everywhere: a knowledge coral may
    # legitimately record a postmortem of an experiment. What differs per
    # profile is what the tier *means*, not which strings parse.
    "prior-art", "postmortem", "spec",
)

# Names that appear in prose, in the protocol and in older clients.
ALIASES = {
    "open-question": "question",
    "open_question": "question",
    "openquestion": "question",
    "priorart": "prior-art",
    "prior_art": "prior-art",
    "post-mortem": "postmortem",
    "post_mortem": "postmortem",
}

# Types that carry a proposition and can therefore be voted on.
PROPOSITIONAL = ("claim", "anomaly", "question", "decision", "prior-art",
                 "postmortem", "spec")


def normalize(raw) -> str:
    """Return the canonical type name, or raise ValueError naming the choices."""
    t = str(raw or "claim").strip().lower()
    t = ALIASES.get(t, t)
    if t not in CANONICAL:
        raise ValueError(
            f"unknown type {raw!r}: use one of {', '.join(CANONICAL)} "
            f"(aliases: {', '.join(sorted(ALIASES))})")
    return t
