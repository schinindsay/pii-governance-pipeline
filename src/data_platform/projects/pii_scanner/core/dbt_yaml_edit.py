"""Insert masking metadata into a dbt schema YAML file without reformatting it."""

import json
import re

import yaml
from yaml.nodes import MappingNode, ScalarNode, SequenceNode

APPLIED = "applied"
CREATED = "created"
ALREADY_SET = "already_set"
MODEL_NOT_FOUND = "model_not_found"
COLUMN_NOT_FOUND = "column_not_found"
EXISTING_CONFIG = "existing_config_manual_merge"
CREATE_UNSUPPORTED = "create_unsupported_manual"

_STRING_TAG = "tag:yaml.org,2002:str"
_NULL_TAG = "tag:yaml.org,2002:null"
_MERGE_TAG = "tag:yaml.org,2002:merge"


def _compose(text):
    try:
        root = yaml.compose(text, Loader=yaml.SafeLoader)
    except yaml.YAMLError as exc:
        raise ValueError("Invalid dbt schema YAML") from exc

    # The composer reuses the anchored node for every alias. Editing an anchored
    # target would also change what all its aliases mean.
    seen, shared = set(), set()
    pending = [root]
    while pending:
        node = pending.pop()
        if node in seen:
            shared.add(node)
            continue
        seen.add(node)
        if isinstance(node, MappingNode):
            for key, value in node.value:
                pending.extend((key, value))
        elif isinstance(node, SequenceNode):
            pending.extend(node.value)
    return root, shared


def _field(mapping, name, shared):
    if not isinstance(mapping, MappingNode) or mapping in shared:
        raise ValueError(f"Expected an unaliased mapping for {name!r}")
    matches = []
    for key, value in mapping.value:
        if isinstance(key, ScalarNode) and key.tag == _MERGE_TAG:
            raise ValueError(f"YAML merge makes {name!r} ambiguous")
        if isinstance(key, ScalarNode) and key.value == name:
            if key in shared:
                raise ValueError(f"Alias-backed YAML key {name!r}")
            matches.append((key, value))
    if len(matches) > 1:
        raise ValueError(f"Duplicate YAML key {name!r}")
    return matches[0] if matches else (None, None)


def _named_entry(sequence, name, shared):
    if sequence in shared:
        raise ValueError("Alias-backed YAML sequence")
    if sequence is None or (
        isinstance(sequence, ScalarNode) and sequence.tag == _NULL_TAG
    ):
        return None
    if not isinstance(sequence, SequenceNode) or (
        sequence.flow_style and sequence.value
    ):
        raise ValueError("Expected an unaliased block-style YAML sequence")

    matches = []
    for item in sequence.value:
        _, value = _field(item, "name", shared)
        if (
            isinstance(value, ScalarNode)
            and value.tag == _STRING_TAG
            and re.fullmatch(re.escape(name), value.value, re.IGNORECASE)
        ):
            if item.flow_style or value in shared:
                raise ValueError(f"Unsupported YAML entry for {name!r}")
            matches.append(item)
    if len(matches) > 1:
        raise ValueError(f"Ambiguous YAML name {name!r}")
    return matches[0] if matches else None


def _locate(text, model, column):
    root, shared = _compose(text)
    if root is None:
        return None, None, None, shared
    _, models = _field(root, "models", shared)
    model_node = _named_entry(models, model, shared)
    if model_node is None:
        return None, None, None, shared
    if root.flow_style:
        raise ValueError("Expected a block-style dbt schema")
    _, columns = _field(model_node, "columns", shared)
    return model_node, columns, _named_entry(columns, column, shared), shared


def _policy(column, shared):
    _, config = _field(column, "config", shared)
    if config in shared:
        raise ValueError("Alias-backed column config")
    if not isinstance(config, MappingNode):
        return None
    _, meta = _field(config, "meta", shared)
    if meta in shared:
        raise ValueError("Alias-backed column metadata")
    if not isinstance(meta, MappingNode):
        return None
    _, policy = _field(meta, "masking_policy", shared)
    if policy in shared:
        raise ValueError("Alias-backed masking policy")
    return policy


def has_masking_policy(yaml_text: str, model: str, column: str) -> bool:
    """Check whether the target column has a configured masking policy."""
    _, _, column_node, shared = _locate(yaml_text, model, column)
    if column_node is None:
        return False
    policy = _policy(column_node, shared)
    return (
        isinstance(policy, ScalarNode)
        and policy.tag != _NULL_TAG
        and bool(policy.value.strip())
    )


def _scalar_text(value):
    if re.fullmatch(r"[A-Za-z_][\w.-]*", value):
        node = yaml.compose(value, Loader=yaml.SafeLoader)
        if node.tag == _STRING_TAG:
            return value
    return json.dumps(value, ensure_ascii=False)


def add_masking_policy(yaml_text: str, model: str, column: str, policy: str):
    """Insert masking metadata at the selected column and return (text, status)."""
    model_node, columns, column_node, shared = _locate(yaml_text, model, column)
    if model_node is None:
        return yaml_text, MODEL_NOT_FOUND

    newline = "\r\n" if "\r\n" in yaml_text else "\n"
    if column_node is None:
        if not isinstance(columns, SequenceNode) or not columns.value:
            return yaml_text, CREATE_UNSUPPORTED
        sibling = columns.value[0]
        dash = sibling.start_mark.column - 2
        line_start = yaml_text.rfind("\n", 0, sibling.start_mark.index) + 1
        if (
            sibling.flow_style
            or dash < 0
            or not re.fullmatch(
                r" *- ", yaml_text[line_start : sibling.start_mark.index]
            )
        ):
            raise ValueError("Cannot determine safe column sequence indentation")
        boundary = columns.end_mark.index
        offset = boundary
        if boundary < len(yaml_text):
            offset = yaml_text.rfind("\n", 0, boundary) + 1
            if offset <= line_start or yaml_text[offset:boundary].strip(" "):
                raise ValueError("Cannot determine safe end of columns sequence")

            # Keep comments at the next key's indentation with that key.
            following_indent = boundary - offset
            previous_end = offset
            while previous_end > line_start:
                previous_start = yaml_text.rfind("\n", 0, previous_end - 1) + 1
                line = yaml_text[previous_start:previous_end]
                if line.strip():
                    stripped = line.lstrip(" ")
                    if (
                        not stripped.startswith("#")
                        or len(line) - len(stripped) > following_indent
                    ):
                        break
                    offset = previous_start
                previous_end = previous_start
        elif not yaml_text.endswith("\n"):
            # Appending a line break can change a final literal or folded scalar.
            last_value = columns.value[-1]
            while (
                isinstance(last_value, (MappingNode, SequenceNode)) and last_value.value
            ):
                if isinstance(last_value, MappingNode):
                    last_value = last_value.value[-1][1]
                else:
                    last_value = last_value.value[-1]
            if isinstance(last_value, ScalarNode) and last_value.style in ("|", ">"):
                raise ValueError(
                    "Cannot append after a block scalar without a final newline"
                )
        previous_line = yaml_text[:offset].rstrip().rsplit("\n", 1)[-1]
        comment = previous_line.lstrip(" ")
        if comment.startswith("#") and len(previous_line) - len(comment) <= dash:
            raise ValueError("Cannot append a column after ambiguous trailing comments")
        indent = sibling.start_mark.column
        insert = [" " * dash + f"- name: {_scalar_text(column)}"]
        status = CREATED
    else:
        current = _policy(column_node, shared)
        if (
            isinstance(current, ScalarNode)
            and current.tag == _STRING_TAG
            and current.value == policy
        ):
            return yaml_text, ALREADY_SET
        if any(
            _field(column_node, key, shared)[0] is not None
            for key in ("config", "meta", "masking_policy")
        ):
            return yaml_text, EXISTING_CONFIG

        name_key, name = _field(column_node, "name", shared)
        if name.end_mark.line != name_key.start_mark.line:
            raise ValueError("Cannot insert after a multiline column name")
        indent = column_node.start_mark.column
        line_end = yaml_text.find("\n", name.end_mark.index)
        offset = line_end + 1 if line_end != -1 else len(yaml_text)
        insert = []
        status = APPLIED

    pad = " " * indent
    insert.extend(
        [
            f"{pad}config:",
            f"{pad}  meta:",
            f"{pad}    masking_policy: {_scalar_text(policy)}",
            f"{pad}    contains_pii: true",
        ]
    )
    block = newline.join(insert)
    if offset == len(yaml_text) and not yaml_text.endswith("\n"):
        block = newline + block
    else:
        block += newline
    edited = yaml_text[:offset] + block + yaml_text[offset:]
    try:
        yaml.safe_load(edited)
    except yaml.YAMLError as exc:
        raise ValueError("Invalid edited dbt schema YAML") from exc
    _, _, target, shared = _locate(edited, model, column)
    actual = _policy(target, shared) if target is not None else None
    if (
        not isinstance(actual, ScalarNode)
        or actual.tag != _STRING_TAG
        or actual.value != policy
    ):
        raise ValueError("Could not verify masking policy on the requested column")
    return edited, status
