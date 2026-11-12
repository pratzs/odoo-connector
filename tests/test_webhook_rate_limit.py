"""
/webhooks/shopify must answer topics it does not process with 200 BEFORE the
per-shop rate limit, so an inventory_levels/update burst never returns 429
(Shopify counts those as webhook failures) and never uses up the minute's
budget that real order webhooks need. app.py needs Redis and secrets at
import, so this reads the handler's source instead of importing it.

Run: python -m pytest tests/
"""
import ast
import os

SRC = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")).read()
TREE = ast.parse(SRC)


def handler_source():
    for node in TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name == "shopify_webhook":
            return ast.get_source_segment(SRC, node)
    raise AssertionError("shopify_webhook not found")


def handled_topics():
    for node in TREE.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "HANDLED_WEBHOOK_TOPICS" for t in node.targets):
            return set(ast.literal_eval(node.value))
    raise AssertionError("HANDLED_WEBHOOK_TOPICS not found")


def test_unhandled_topics_return_before_rate_limit():
    src = handler_source()
    assert src.index("topic not in HANDLED_WEBHOOK_TOPICS") < src.index("conn.incr(")


def test_inventory_and_product_topics_are_not_handled():
    topics = handled_topics()
    assert "inventory_levels/update" not in topics
    assert "products/update" not in topics


def test_every_processed_topic_is_in_the_handled_set():
    src = handler_source()
    processed = {"orders/create", "orders/updated", "orders/paid", "orders/cancelled", "refunds/create"}
    for t in processed:
        assert f"'{t}'" in src
    assert processed <= handled_topics()
