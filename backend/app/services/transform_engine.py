"""字段转换引擎：把真实采集样本代入工序关联的转换规则，按依赖次序求值。

核心约定：

* 目标字段之间的依赖构造成有向图，按拓扑次序执行；成环的字段只标记阻塞，
  绝不用空值冒充计算结果。
* 同一条样本里同一目标字段最多交付一个正式值；其余候选分支只留在预演视图里。
* 清单冻结样本身份、样本值、模板版本和每条规则的实际版本；正式转换只消费清单。
* 依赖修复后按“规则快照差异 + 下游依赖闭包”计算重跑范围，未受影响的字段不重跑。
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any, Iterable

from app import persistence as persist
from app.store import store
from app.services import transform_state as state_store

# 样本记录里不属于字段值的元信息键
_SAMPLE_META_KEYS = {
    "样本编号", "sample_id", "编号", "id", "来源模板", "模板编号", "模板版本",
    "关联脚本", "关联工序", "pipeline_code", "pipeline_codes", "创建时间",
    "字段值", "样本值", "values", "fields", "数据", "sample_values", "sample",
    "样本", "来源", "采集时间", "来源行", "行号", "备注",
}

_EMPTY_TOKENS = {"", "-", "—", "无", "none", "null", "~", "空"}

def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _is_present(value: Any) -> bool:
    """空值判定：0、False 是有效值，空串/None 视为缺失。"""
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().casefold() not in _EMPTY_TOKENS
    return True


def split_fields(raw: Any) -> list[str]:
    """把“字段A,字段B”之类的配置拆成字段名列表。"""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        items = [str(item).strip() for item in raw]
    else:
        text = str(raw).strip()
        if text.casefold() in _EMPTY_TOKENS:
            return []
        items = [part.strip() for part in re.split(r"[,，、;；|\n\r]+", text)]
    return [item for item in items if item and item.casefold() not in _EMPTY_TOKENS]


def _sample_values(sample: dict[str, Any] | None) -> dict[str, Any]:
    """从各种样本形态里提取真正的字段值。"""
    if not isinstance(sample, dict):
        return {}
    values: dict[str, Any] = {}
    for key in ("字段值", "样本值", "values", "fields", "数据", "sample_values",
                "采集行", "sample_row", "row", "record"):
        nested = sample.get(key)
        if isinstance(nested, dict):
            values.update(copy.deepcopy(nested))
    nested_sample = sample.get("sample") or sample.get("样本")
    if isinstance(nested_sample, dict):
        values.update({
            key: copy.deepcopy(value)
            for key, value in nested_sample.items()
            if key not in _SAMPLE_META_KEYS
        })
    for key, value in sample.items():
        if key not in _SAMPLE_META_KEYS and not key.startswith("_"):
            if not isinstance(value, (dict, list)):
                values.setdefault(key, copy.deepcopy(value))
    return values


def normalize_sample(sample: dict[str, Any] | None) -> dict[str, Any]:
    """标准化为保存用的样本记录。"""
    sample = sample or {}
    values = _sample_values(sample)
    record = {
        "样本编号": _text(sample.get("样本编号") or sample.get("sample_id")),
        "字段值": values,
        "样本值": values,
        "模板版本": _text(sample.get("模板版本") or sample.get("template_version")),
        "来源模板": _text(sample.get("来源模板") or sample.get("模板编号") or sample.get("template_id")),
        "关联脚本": _text(sample.get("关联脚本") or sample.get("script_id")),
        "来源": _text(sample.get("来源") or sample.get("source")),
        "创建时间": _text(sample.get("创建时间") or sample.get("timestamp")),
    }
    links = sample.get("关联工序") or sample.get("pipeline_codes") or sample.get("pipeline_code")
    if links:
        record["关联工序"] = links
    for key, value in sample.items():
        if key.startswith("_") or key in record:
            continue
        if key in ("sample", "样本") and isinstance(value, dict):
            continue
        record[key] = copy.deepcopy(value)
    return record


def sample_identity(sample: dict[str, Any] | None) -> str:
    record = normalize_sample(sample)
    if record["样本编号"]:
        return record["样本编号"]
    digest = hashlib.sha1(
        json.dumps(record["字段值"], ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()[:10]
    return f"SAMPLE-{digest}"


def _rule_version(rule: dict[str, Any]) -> str:
    return _text(rule.get("规则版本")) or _text(rule.get("version")) or f"rule-{rule.get('id', '')}"


def _rule_fingerprint(rule: dict[str, Any]) -> str:
    payload = {
        "rule": _text(rule.get("规则编号")),
        "version": _rule_version(rule),
        "source": split_fields(rule.get("源字段")),
        "target": _text(rule.get("目标字段")) or _text(rule.get("源字段")),
        "func": _text(rule.get("转换函数")),
        "deps": split_fields(rule.get("依赖字段")),
        "null": _text(rule.get("空值处理")),
        "result": _text(rule.get("结果值") or rule.get("固定值")),
    }
    return hashlib.md5(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()[:12]


def rule_snapshot(rule: dict[str, Any]) -> dict[str, Any]:
    """清单里保留的规则快照（含实际版本和可回查的条目指纹）。"""
    target = _text(rule.get("目标字段")) or _text(rule.get("源字段"))
    return {
        "规则编号": _text(rule.get("规则编号")) or f"TRAN-{rule.get('id')}",
        "规则ID": rule.get("id"),
        "规则版本": _rule_version(rule),
        "实际规则版本": _rule_version(rule),
        "条目版本": _text(rule.get("version")),
        "实际版本": _rule_version(rule) or _text(rule.get("version")),
        "规则状态": rule.get("status", ""),
        "源字段": _text(rule.get("源字段")),
        "目标字段": target,
        "依赖字段": _text(rule.get("依赖字段")),
        "转换函数": _text(rule.get("转换函数")),
        "空值处理": _text(rule.get("空值处理")),
        "指纹": _rule_fingerprint(rule),
    }


def collect_rules(
    pipeline_code: str,
    pipeline_id: int | None = None,
    rule_ids: Iterable[Any] | None = None,
) -> list[dict[str, Any]]:
    """取出工序关联且未废弃的转换规则，按执行顺序稳定排序。"""
    wanted: set[str] = set()
    if rule_ids:
        for item in rule_ids:
            if isinstance(item, dict):
                wanted.add(_text(item.get("规则编号") or item.get("id")))
            else:
                wanted.add(_text(item))
    links = {_text(pipeline_code)}
    if pipeline_id is not None:
        links.add(str(pipeline_id))
    links.discard("")

    picked: list[dict[str, Any]] = []
    for rule in store.rows("transform_rule"):
        if _text(rule.get("status")) == "已废弃":
            continue
        linked = _text(rule.get("关联工序"))
        if linked not in links:
            continue
        if wanted:
            identifiers = {
                _text(rule.get("规则编号")), _text(rule.get("id")),
                _text(rule.get("规则版本")),
            }
            if not identifiers & wanted:
                continue
        picked.append(rule)

    def _sort_key(rule: dict[str, Any]) -> tuple[int, int, int]:
        priority = 0
        if _text(rule.get("status")) == "已生效":
            priority = -1
        explicit = _text(rule.get("执行顺序") or rule.get("优先级"))
        try:
            explicit_order = int(explicit) if explicit else 10_000
        except ValueError:
            explicit_order = 10_000
        return priority, explicit_order, int(rule.get("id", 0) or 0)

    return sorted(picked, key=_sort_key)


def _parse_mapping(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return {str(key): value for key, value in raw.items()}
    if not raw:
        return {}
    mapping: dict[str, Any] = {}
    for part in re.split(r"[,，;；\n]+", str(raw)):
        part = part.strip()
        if not part:
            continue
        for sep in ("=>", "->", "=", ">", ":", "："):
            if sep in part:
                key, value = part.split(sep, 1)
                mapping[key.strip()] = value.strip()
                break
    return mapping


def _compute_rule(rule: dict[str, Any], values: dict[str, Any]) -> Any:
    """执行一条规则；返回 None 表示转换结果为空（调用方会按阻塞处理）。"""
    sources = split_fields(rule.get("源字段"))
    deps = split_fields(rule.get("依赖字段"))
    reads = [*sources, *[dep for dep in deps if dep not in sources]]

    explicit = rule.get("结果值")
    if _is_present(explicit):
        return explicit
    fixed = rule.get("固定值")
    if _is_present(fixed):
        return fixed

    function = _text(rule.get("转换函数"))
    lowered = function.casefold()
    first_source = sources[0] if sources else (reads[0] if reads else "")
    source_value = values.get(first_source) if first_source else None

    # 常量 / 固定值：支持“固定值:甲”“常量=甲”“literal:甲”
    for prefix in ("固定值", "常量", "固定", "literal", "const", "constant"):
        if lowered.startswith(prefix.casefold()):
            rest = function[len(prefix):].lstrip(" :：=->")
            if rest:
                return rest

    # 映射：支持“映射:A=甲”“map:A>甲”，或单独的映射表字段
    mapping = _parse_mapping(rule.get("映射表") or rule.get("映射"))
    mapping_like = (
        "映射" in function or lowered.startswith("map") or bool(mapping)
        or "->" in function or "=>" in function
    )
    if mapping_like:
        if not mapping:
            rest = function
            for prefix in ("映射", "map"):
                if lowered.startswith(prefix):
                    rest = function[len(prefix):]
                    break
            rest = rest.lstrip(" :：=")
            mapping = _parse_mapping(rest)
        if mapping:
            key = _text(source_value)
            if key in mapping:
                return mapping[key]
            default = rule.get("映射默认值") or rule.get("默认值")
            if _is_present(default):
                return default
            # 没有命中映射时等同于直接取值，避免用空值冒充结果
            return source_value

    if "拼接" in function or lowered.startswith(("concat", "join")):
        separator = _text(rule.get("分隔符")) or "-"
        parts = [values.get(field) for field in reads]
        if any(not _is_present(part) for part in parts):
            return None
        return separator.join(str(part) for part in parts)

    if "截取" in function or lowered.startswith(("substr", "slice")):
        if not _is_present(source_value):
            return None
        match = re.search(r"(\d+)\s*[:：,，]\s*(\d+)", function)
        if match:
            start, end = int(match.group(1)), int(match.group(2))
            return str(source_value)[start:end]
        return str(source_value)

    if function in {"大写", "upper", "uppercase"}:
        return str(source_value).upper() if _is_present(source_value) else None
    if function in {"小写", "lower", "lowercase"}:
        return str(source_value).lower() if _is_present(source_value) else None
    if function in {"去空格", "trim", "strip"}:
        return str(source_value).strip() if _is_present(source_value) else None

    # copy / 直接复制 / 透传 / 空函数：取源字段原值
    if len(sources) > 1:
        separator = _text(rule.get("分隔符")) or "-"
        parts = [values.get(field) for field in sources]
        if any(not _is_present(part) for part in parts):
            return None
        return separator.join(str(part) for part in parts)
    if source_value is not None:
        return source_value
    # 没有源字段时退化为第一个依赖字段的值
    if reads:
        return values.get(reads[0])
    return None


def _target_graph(rules: list[dict[str, Any]]) -> tuple[list[str], set[str], dict[str, set[str]]]:
    """构造目标字段依赖图，返回目标顺序、成环字段和环成员。"""
    targets: list[str] = []
    for rule in rules:
        target = _text(rule.get("目标字段")) or _text(rule.get("源字段"))
        if target and target not in targets:
            targets.append(target)
    target_set = set(targets)
    edges: dict[str, set[str]] = {target: set() for target in targets}
    for rule in rules:
        target = _text(rule.get("目标字段")) or _text(rule.get("源字段"))
        if not target:
            continue
        reads = [*split_fields(rule.get("源字段")), *split_fields(rule.get("依赖字段"))]
        for read in reads:
            # 目标字段读取自身代表“取已抽取的原值”，不构成目标间依赖环
            if read in target_set and read != target:
                edges[target].add(read)

    # Tarjan 强连通分量，找出成环字段
    index = 0
    indices: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    cycles: set[str] = set()
    cycle_peers: dict[str, set[str]] = {}

    def connect(node: str) -> None:
        nonlocal index
        indices[node] = index
        low[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)
        for neighbor in edges.get(node, ()):
            if neighbor not in indices:
                connect(neighbor)
                low[node] = min(low[node], low[neighbor])
            elif neighbor in on_stack:
                low[node] = min(low[node], indices[neighbor])
        if low[node] == indices[node]:
            component: list[str] = []
            while True:
                member = stack.pop()
                on_stack.discard(member)
                component.append(member)
                if member == node:
                    break
            is_cycle = len(component) > 1 or (
                len(component) == 1 and node in edges.get(node, set())
            )
            if is_cycle:
                for member in component:
                    cycles.add(member)
                    cycle_peers[member] = set(component) - {member}

    for target in targets:
        if target not in indices:
            connect(target)
    return targets, cycles, cycle_peers


def _execution_order(
    targets: list[str], edges: dict[str, set[str]], cycles: set[str]
) -> list[str]:
    """Kahn 拓扑排序；成环/依赖阻塞的字段按首次出现顺序追加。"""
    indegree = {target: 0 for target in targets}
    for target, reads in edges.items():
        indegree[target] = len({read for read in reads if read in indegree and read != target})
    ready = [target for target in targets if indegree.get(target, 0) == 0]
    order: list[str] = []
    seen: set[str] = set()
    while ready:
        node = ready.pop(0)
        if node in seen:
            continue
        seen.add(node)
        order.append(node)
        for target in targets:
            if target in seen or node not in edges.get(target, set()):
                continue
            indegree[target] = max(0, indegree.get(target, 0) - 1)
            if indegree[target] == 0:
                ready.append(target)
    for target in targets:
        if target not in seen:
            order.append(target)
    return order


def _entry_for_rule(
    rule: dict[str, Any],
    *,
    order: int,
    status: str,
    value: Any,
    blocked: list[str],
    missing: list[str],
    deliver: bool,
    note: str,
    cycles: set[str],
) -> dict[str, Any]:
    snapshot = rule_snapshot(rule)
    target = snapshot["目标字段"]
    return {
        "执行序号": order,
        "序号": order,
        "目标字段": target,
        "字段": target,
        "规则编号": snapshot["规则编号"],
        "规则版本": snapshot["规则版本"],
        "条目版本": snapshot["条目版本"],
        "规则状态": snapshot["规则状态"],
        "源字段": snapshot["源字段"],
        "依赖字段": snapshot["依赖字段"],
        "转换函数": snapshot["转换函数"],
        "状态": status,
        "可计算": status in {"可计算", "重复目标"},
        "结果值": value if status in {"可计算", "重复目标"} else None,
        "值": value if status in {"可计算", "重复目标"} else None,
        "正式交付": deliver,
        "交付": deliver,
        "阻塞依赖": blocked,
        "缺失依赖": missing,
        "阻塞原因": note,
        "成环": target in cycles,
        "指纹": snapshot["指纹"],
    }


def evaluate_rules(
    rules: list[dict[str, Any]],
    sample: dict[str, Any] | None,
) -> dict[str, Any]:
    """按依赖次序执行规则，返回字段序列、可计算值、阻塞依赖和执行次序。"""
    values = _sample_values(sample)
    targets, cycles, cycle_peers = _target_graph(rules)
    target_set = set(targets)

    edges: dict[str, set[str]] = {target: set() for target in targets}
    rules_by_target: dict[str, list[dict[str, Any]]] = {target: [] for target in targets}
    for rule in rules:
        target = _text(rule.get("目标字段")) or _text(rule.get("源字段"))
        if not target:
            continue
        rules_by_target.setdefault(target, []).append(rule)
        reads = [*split_fields(rule.get("源字段")), *split_fields(rule.get("依赖字段"))]
        for read in reads:
            if read in target_set and read != target:
                edges[target].add(read)

    execution = _execution_order(targets, edges, cycles)
    resolved: dict[str, Any] = {}
    blocked_targets: dict[str, list[str]] = {}
    entries: list[dict[str, Any]] = []
    deliverable: dict[str, Any] = {}
    blocked_map: dict[str, list[str]] = {}
    sequence = 0

    for target in execution:
        candidates = rules_by_target.get(target, [])
        computed: list[tuple[dict[str, Any], Any]] = []
        branch_entries: list[dict[str, Any]] = []
        target_blocked: list[str] = []
        target_missing: list[str] = []

        for rule in candidates:
            sequence += 1
            sources = split_fields(rule.get("源字段"))
            deps = split_fields(rule.get("依赖字段"))
            reads: list[str] = []
            for field in [*sources, *deps]:
                if field not in reads:
                    reads.append(field)

            blocked: list[str] = []
            missing: list[str] = []
            if target in cycles:
                blocked.extend(sorted(cycle_peers.get(target, set())))
            for read in reads:
                if read in target_set:
                    # 目标读取自身 = 取已抽取的原值，不算阻塞也不算环
                    if read == target:
                        continue
                    if read in resolved:
                        continue
                    blocked.append(read)
                elif not _is_present(values.get(read)):
                    missing.append(read)

            blocked = list(dict.fromkeys(blocked))
            missing = list(dict.fromkeys(missing))
            if blocked or missing:
                reasons = []
                if blocked:
                    if any(dep in cycles for dep in blocked):
                        reasons.append(f"依赖成环：{'、'.join(blocked)}")
                    else:
                        reasons.append(f"上游字段不可计算：{'、'.join(blocked)}")
                if missing:
                    reasons.append(f"依赖缺失：{'、'.join(missing)}")
                note = "；".join(reasons)
                entry = _entry_for_rule(
                    rule, order=sequence, status="阻塞", value=None,
                    blocked=blocked, missing=missing, deliver=False,
                    note=note, cycles=cycles,
                )
                branch_entries.append(entry)
                for field in blocked:
                    if field not in target_blocked:
                        target_blocked.append(field)
                for field in missing:
                    if field not in target_missing:
                        target_missing.append(field)
                continue

            result = _compute_rule(rule, {**values, **resolved})
            if not _is_present(result):
                missing_fields = [
                    field for field in reads if not _is_present(values.get(field))
                    and field not in target_set
                ]
                note = "转换结果为空，不作为计算结果"
                if missing_fields:
                    note = f"依赖缺失：{'、'.join(missing_fields)}；{note}"
                entry = _entry_for_rule(
                    rule, order=sequence, status="阻塞", value=None,
                    blocked=[], missing=missing_fields, deliver=False,
                    note=note, cycles=cycles,
                )
                branch_entries.append(entry)
                for field in missing_fields:
                    if field not in target_missing:
                        target_missing.append(field)
                continue

            entry = _entry_for_rule(
                rule, order=sequence, status="可计算", value=result,
                blocked=[], missing=[], deliver=False, note="", cycles=cycles,
            )
            computed.append((rule, result))
            branch_entries.append(entry)

        if target_blocked or target_missing:
            blocked_map[target] = list(dict.fromkeys(target_blocked + target_missing))
        if computed:
            primary_rule, primary_value = computed[0]
            primary_id = _text(primary_rule.get("规则编号"))
            resolved[target] = primary_value
            deliverable[target] = primary_value
            for entry in branch_entries:
                if entry["状态"] == "可计算":
                    if entry["规则编号"] == primary_id:
                        entry["正式交付"] = True
                        entry["交付"] = True
                    else:
                        entry["状态"] = "重复目标"
                        entry["可计算"] = True
                        entry["正式交付"] = False
                        entry["交付"] = False
                        entry["阻塞原因"] = (
                            f"同一目标已由规则{primary_id}交付，正式结果只保留一个值"
                        )
        else:
            resolved.pop(target, None)
            blocked_targets[target] = list(dict.fromkeys(target_blocked + target_missing))
            if target not in blocked_map:
                blocked_map[target] = blocked_targets[target]

        entries.extend(branch_entries)

    return {
        "字段执行序列": entries,
        "执行次序": execution,
        "可计算字段": deliverable,
        "正式值": deliverable,
        "阻塞依赖": blocked_map,
        "阻塞字段": blocked_targets,
        "无值阻塞字段": blocked_targets,
        "样本值": values,
        "成环字段": sorted(cycles),
        "规则数": len(rules),
    }


def _rule_rows_from_checklist(checklist: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for item in checklist.get("规则清单", []):
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _target_rule_fingerprints(rule_rows: list[dict[str, Any]]) -> dict[str, set[str]]:
    grouped: dict[str, set[str]] = {}
    for item in rule_rows:
        target = _text(item.get("目标字段"))
        if not target:
            continue
        fingerprint = _text(item.get("指纹")) or hashlib.md5(
            json.dumps(item, ensure_ascii=False, sort_keys=True, default=str).encode()
        ).hexdigest()[:12]
        grouped.setdefault(target, set()).add(fingerprint)
    return grouped


def _downstream_targets(rules: list[dict[str, Any]], seeds: set[str]) -> set[str]:
    targets = {
        _text(rule.get("目标字段")) or _text(rule.get("源字段")) for rule in rules
    }
    targets.discard("")
    affected = set(seeds)
    changed = True
    while changed:
        changed = False
        for rule in rules:
            target = _text(rule.get("目标字段")) or _text(rule.get("源字段"))
            if not target or target in affected:
                continue
            reads = [*split_fields(rule.get("源字段")), *split_fields(rule.get("依赖字段"))]
            if any(read in affected for read in reads):
                affected.add(target)
                changed = True
    return affected & targets


def compute_rerun_scope(
    previous_checklist: dict[str, Any] | None,
    current_rules: list[dict[str, Any]],
    current_preview: dict[str, Any],
) -> list[str]:
    """对比上一份正式清单和新规则，算出受影响字段（含下游闭包）。"""
    if not previous_checklist:
        # 首次清单的重跑范围即可交付字段；阻塞字段在依赖补齐后进入后续重跑
        return list(current_preview.get("可计算字段", {}).keys()) or [
            entry["目标字段"] for entry in current_preview.get("字段执行序列", [])
            if entry.get("正式交付")
        ]
    old_rows = _rule_rows_from_checklist(previous_checklist)
    old_by_target = _target_rule_fingerprints(old_rows)
    new_rows = [rule_snapshot(rule) for rule in current_rules]
    new_by_target = _target_rule_fingerprints(new_rows)

    changed: set[str] = set()
    for target in set(old_by_target) | set(new_by_target):
        if old_by_target.get(target, set()) != new_by_target.get(target, set()):
            changed.add(target)
    old_blocked = set(
        (previous_checklist.get("阻塞字段") or previous_checklist.get("阻塞依赖") or {}).keys()
    )
    new_blocked = set(
        (current_preview.get("阻塞字段") or current_preview.get("阻塞依赖") or {}).keys()
    )
    # 已经可算出值、但仍带有阻塞分支的目标，在阻塞依赖补齐后也需要重跑
    for target, deps in (current_preview.get("阻塞依赖") or {}).items():
        old_side = (previous_checklist.get("阻塞依赖") or {}).get(target)
        if deps and old_side != deps and target in new_by_target:
            changed.add(target)
    changed |= old_blocked & set(new_by_target)
    changed |= old_blocked - new_blocked
    # 新出现的字段也要进入本轮
    changed |= set(new_by_target) - set(old_by_target)

    if not changed:
        return []
    affected = _downstream_targets(current_rules, changed)
    # 只保留当前仍然存在的目标字段
    current_targets = set(new_by_target)
    return [target for target in current_preview.get("执行次序", []) if target in affected and target in current_targets]


def build_checklist(
    pipeline: dict[str, Any],
    sample: dict[str, Any],
    *,
    version_number: int,
    version_label: str,
    rules: list[dict[str, Any]],
    preview: dict[str, Any],
    rerun_scope: list[str],
    previous_checklist: dict[str, Any] | None = None,
    pruned_rules: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    pipeline_code = _text(pipeline.get("工序编号")) or str(pipeline.get("id", ""))
    sample_id = _text(sample.get("样本编号")) or sample_identity(sample)
    rule_versions = {_text(rule.get("规则编号")): _rule_version(rule) for rule in rules}
    rule_rows = [rule_snapshot(rule) for rule in rules]
    delivered = {
        entry["目标字段"]: entry["结果值"]
        for entry in preview["字段执行序列"]
        if entry.get("正式交付")
    }
    return {
        "清单编号": f"CHK-{pipeline_code}-{version_label}",
        "版本": version_label,
        "版本号": version_number,
        "用途": "字段转换执行清单",
        "工序编号": pipeline_code,
        "工序ID": pipeline.get("id"),
        "样本编号": sample_id,
        "样本身份": sample_id,
        "样本值": copy.deepcopy(preview.get("样本值", {})),
        "模板版本": _text(sample.get("模板版本")),
        "来源模板": _text(sample.get("来源模板")),
        "来源": _text(sample.get("来源")),
        "关联脚本": _text(sample.get("关联脚本")),
        "规则版本": rule_versions,
        "实际规则版本": dict(rule_versions),
        "规则条目版本": {
            snapshot["规则编号"]: snapshot["条目版本"] for snapshot in rule_rows
        },
        "规则清单": rule_rows,
        "已废弃规则": [
            rule_snapshot(rule) for rule in (pruned_rules or [])
        ],
        "字段执行序列": copy.deepcopy(preview["字段执行序列"]),
        "执行次序": list(preview["执行次序"]),
        "可计算字段": copy.deepcopy(preview["可计算字段"]),
        "正式值": delivered,
        "阻塞依赖": copy.deepcopy(preview["阻塞依赖"]),
        "阻塞字段": copy.deepcopy(preview.get("阻塞字段", {})),
        "重跑范围": list(rerun_scope),
        "首次清单": previous_checklist is None,
        "上一清单编号": _text((previous_checklist or {}).get("清单编号")),
        "上一清单版本": _text((previous_checklist or {}).get("版本")),
        "生成时间": persist.now_iso(),
    }


def resolve_sample(
    pipeline: dict[str, Any],
    body: dict[str, Any] | None,
) -> dict[str, Any]:
    """从请求体或已登记样本里取真实采集行，缺样本编号时补一个。"""
    body = body or {}
    pipeline_code = _text(pipeline.get("工序编号")) or str(pipeline.get("id", ""))
    for key in ("sample", "样本", "sample_row", "样本行", "采集行", "采集样本",
                "raw_row", "extracted_row", "actual_sample", "source_row",
                "row", "record"):
        candidate = body.get(key)
        if isinstance(candidate, dict):
            sample = normalize_sample(candidate)
            if not sample["样本编号"]:
                sample["样本编号"] = sample_identity(sample)
            if not sample.get("关联工序"):
                sample["关联工序"] = [pipeline_code]
            return state_store.save_sample(sample)
    if _text(body.get("sample_id") or body.get("样本编号")):
        sample_id = _text(body.get("sample_id") or body.get("样本编号"))
        stored = state_store.get_sample(sample_id)
        if stored:
            return stored
    if (body.get("values") or body.get("字段值") or body.get("样本值")
            or body.get("sample_values") or body.get("fields")):
        sample = normalize_sample(body)
        sample["样本编号"] = _text(
            body.get("sample_id") or body.get("样本编号")
        ) or sample_identity(sample)
        sample["关联工序"] = sample.get("关联工序") or [pipeline_code]
        return state_store.save_sample(sample)
    control_keys = {
        "action", "remark", "sample_id", "样本编号", "values", "字段值", "样本值",
        "sample_values", "fields", "rule_ids", "rules", "rerun", "rerun_scope",
        "重跑范围", "previous_version", "上一清单版本", "previous_checklist",
        "checklist", "checklist_id", "checklist_version", "清单版本", "工序编号",
        "pipeline_code", "dry_run", "sample", "样本",
    }
    direct = {
        key: copy.deepcopy(value) for key, value in body.items()
        if key not in control_keys and not key.startswith("_")
        and not isinstance(value, (dict, list))
    }
    if direct:
        sample = normalize_sample({
            "样本编号": _text(body.get("sample_id") or body.get("样本编号")),
            "字段值": direct,
            "模板版本": _text(body.get("模板版本") or body.get("template_version")),
        })
        if not sample["样本编号"]:
            sample["样本编号"] = sample_identity(sample)
        sample["关联工序"] = [pipeline_code]
        return state_store.save_sample(sample)
    stored = state_store.latest_sample(pipeline_code, pipeline.get("id"))
    if stored:
        return stored
    raise ValueError("缺少真实采集样本：请先提交样本值或 data_extract 的样本编号")


def _find_previous_formal(
    pipeline_state: dict[str, Any], sample_id: str
) -> dict[str, Any] | None:
    for result in reversed(pipeline_state.get("results", [])):
        if _text(result.get("样本编号")) == sample_id and result.get("清单编号"):
            return state_store.find_checklist(
                pipeline_state, checklist_id=result.get("清单编号")
            )
    return None


def prepare_checklist(
    pipeline: dict[str, Any],
    body: dict[str, Any] | None = None,
    *,
    sample: dict[str, Any] | None = None,
    rules: list[dict[str, Any]] | None = None,
    rerun: bool | None = None,
    persist_checklist: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """按当前规则生成一份冻结的执行清单，并给出预演视图。"""
    body = body or {}
    pipeline_state = state_store.get_pipeline_state(
        int(pipeline.get("id", 0)), _text(pipeline.get("工序编号"))
    )
    if sample is None:
        sample = resolve_sample(pipeline, body)
    sample = normalize_sample(sample)
    if not sample["样本编号"]:
        sample["样本编号"] = sample_identity(sample)
    if not sample.get("关联工序"):
        sample["关联工序"] = [_text(pipeline.get("工序编号"))]
    # 预演/正式转换用到的样本都登记下来，后续可按样本编号复用
    sample = state_store.save_sample(sample)

    if rules is None:
        rule_ids = body.get("rule_ids") or body.get("规则") or body.get("rules")
        rules = collect_rules(
            _text(pipeline.get("工序编号")), pipeline.get("id"), rule_ids
        )
    preview = evaluate_rules(rules, sample)

    previous_formal = _find_previous_formal(pipeline_state, sample["样本编号"])
    previous_for_scope = previous_formal
    if body.get("previous_checklist") and isinstance(body["previous_checklist"], dict):
        previous_for_scope = body["previous_checklist"]
    elif body.get("previous_version") or body.get("上一清单版本"):
        found = state_store.find_checklist(
            pipeline_state,
            version=body.get("previous_version") or body.get("上一清单版本"),
        )
        previous_for_scope = found or previous_for_scope

    if rerun is False:
        scope = list(preview["可计算字段"].keys())
    else:
        scope = compute_rerun_scope(previous_for_scope, rules, preview)
    if rerun is True and previous_for_scope is None:
        scope = list(preview["可计算字段"].keys())

    if persist_checklist:
        version_number, version_label = state_store.next_checklist_version(pipeline_state)
    else:
        version_number, version_label = 0, "预演"
    checklist = build_checklist(
        pipeline, sample,
        version_number=version_number,
        version_label=version_label,
        rules=rules,
        preview=preview,
        rerun_scope=scope,
        previous_checklist=previous_for_scope if body.get("previous_checklist") else None,
    )
    if persist_checklist:
        pipeline_state.setdefault("checklists", []).append(copy.deepcopy(checklist))
        state_store.save_pipeline_state(pipeline_state)
        persist.append_jsonl("reports", "transform_checklists.jsonl", {
            **copy.deepcopy(checklist),
            "pipeline_id": pipeline.get("id"),
            "timestamp": persist.now_iso(),
        })
    return checklist, preview, pipeline_state


def formal_transform(
    pipeline: dict[str, Any],
    checklist: dict[str, Any],
    *,
    sample: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """正式转换：只消费清单，写出一份结果并落交付台账。"""
    pipeline_state = state_store.get_pipeline_state(
        int(pipeline.get("id", 0)), _text(pipeline.get("工序编号"))
    )
    sample_id = _text(checklist.get("样本编号"))
    if sample is None:
        sample = state_store.get_sample(sample_id) or {
            "样本编号": sample_id,
            "字段值": checklist.get("样本值", {}),
            "模板版本": checklist.get("模板版本", ""),
        }
    sample = normalize_sample(sample)

    scope_declared = "重跑范围" in checklist
    scope = list(checklist.get("重跑范围") or [])
    scope_set = set(scope)
    all_entries = checklist.get("字段执行序列", [])
    # 同一目标只取一条正式候选（清单里已标记正式交付，这里再兜底去重）
    formal_values: dict[str, Any] = {}
    delivered_entries: list[dict[str, Any]] = []
    seen_targets: set[str] = set()
    for entry in all_entries:
        if not entry.get("正式交付"):
            continue
        target = _text(entry.get("目标字段"))
        if not target or target in seen_targets:
            continue
        if scope_declared and target not in scope_set:
            continue
        seen_targets.add(target)
        formal_values[target] = entry.get("结果值")
        delivered_entries.append(entry)

    # 阻塞依赖是当前清单的真实状态，不随重跑范围收窄
    blocked = {
        target: deps for target, deps in (checklist.get("阻塞字段") or {}).items()
    }
    branch_blocked = {
        target: deps for target, deps in (checklist.get("阻塞依赖") or {}).items()
    }

    # 交付台账：同一目标旧的正式值保留为“已替代”，历史仍可回查
    deliveries = pipeline_state.setdefault("deliveries", [])
    previous_active: dict[str, dict[str, Any]] = {}
    for item in deliveries:
        if (
            _text(item.get("样本编号")) == sample_id
            and item.get("状态", "有效") == "有效"
        ):
            previous_active[_text(item.get("目标字段"))] = item

    result_id = f"TRES-{pipeline.get('id', 0)}-{len(pipeline_state.get('results', [])) + 1:04d}"
    new_deliveries: list[dict[str, Any]] = []
    for entry in delivered_entries:
        target = _text(entry.get("目标字段"))
        old = previous_active.get(target)
        if old is not None:
            old["状态"] = "已替代"
            old["替代时间"] = persist.now_iso()
            old["替代结果"] = result_id
        delivery = {
            "交付编号": f"DEL-{pipeline.get('id', 0)}-{len(deliveries) + 1:04d}",
            "结果编号": result_id,
            "清单编号": checklist.get("清单编号"),
            "清单版本": checklist.get("版本"),
            "样本编号": sample_id,
            "目标字段": target,
            "值": entry.get("结果值"),
            "规则编号": entry.get("规则编号"),
            "规则版本": entry.get("规则版本"),
            "条目版本": entry.get("条目版本"),
            "状态": "有效",
            "交付时间": persist.now_iso(),
        }
        deliveries.append(delivery)
        new_deliveries.append(copy.deepcopy(delivery))

    active: dict[str, Any] = {}
    history: list[dict[str, Any]] = []
    for item in deliveries:
        if _text(item.get("样本编号")) != sample_id:
            continue
        history.append(copy.deepcopy(item))
        if item.get("状态", "有效") == "有效":
            active[_text(item.get("目标字段"))] = item.get("值")

    result = {
        "结果编号": result_id,
        "清单编号": checklist.get("清单编号"),
        "清单版本": checklist.get("版本"),
        "样本编号": sample_id,
        "模板版本": checklist.get("模板版本", ""),
        "工序编号": checklist.get("工序编号", ""),
        "字段值": dict(active),
        "正式值": dict(active),
        "本次字段值": dict(formal_values),
        "本次交付": new_deliveries,
        "可交付字段": list(formal_values.keys()),
        "阻塞依赖": copy.deepcopy(blocked),
        "分支阻塞依赖": copy.deepcopy(branch_blocked),
        "重跑范围": list(scope),
        "未参与重跑": [
            target for target in active if target not in scope_set
        ] if scope_set else [],
        "交付记录": history,
        "规则版本": copy.deepcopy(checklist.get("规则版本", {})),
        "执行时间": persist.now_iso(),
    }
    pipeline_state.setdefault("results", []).append(copy.deepcopy(result))
    pipeline_state["rerun_scope"] = list(scope)
    pipeline_state["last_checklist_id"] = checklist.get("清单编号")
    pipeline_state["last_result_id"] = result_id
    state_store.save_pipeline_state(pipeline_state)

    # 分别持久化可交付字段、阻塞依赖和重跑范围，便于审计
    persist.append_jsonl("reports", "transform_results.jsonl", {
        "pipeline_id": pipeline.get("id"),
        "pipeline_code": pipeline.get("工序编号"),
        "result_id": result_id,
        "checklist_id": checklist.get("清单编号"),
        "checklist_version": checklist.get("版本"),
        "sample_id": sample_id,
        "template_version": checklist.get("模板版本", ""),
        "delivered_fields": list(formal_values.keys()),
        "delivered_values": formal_values,
        "blocked_dependencies": blocked,
        "rerun_scope": scope,
        "timestamp": persist.now_iso(),
    })
    persist.append_jsonl("reports", "transform_delivered_fields.jsonl", {
        "pipeline_id": pipeline.get("id"),
        "pipeline_code": pipeline.get("工序编号"),
        "result_id": result_id,
        "checklist_id": checklist.get("清单编号"),
        "sample_id": sample_id,
        "delivered_fields": list(formal_values.keys()),
        "formal_values": formal_values,
        "timestamp": persist.now_iso(),
    })
    persist.append_jsonl("reports", "transform_blocked_dependencies.jsonl", {
        "pipeline_id": pipeline.get("id"),
        "pipeline_code": pipeline.get("工序编号"),
        "result_id": result_id,
        "checklist_id": checklist.get("清单编号"),
        "sample_id": sample_id,
        "blocked_dependencies": blocked,
        "timestamp": persist.now_iso(),
    })
    for delivery in new_deliveries:
        persist.append_jsonl("reports", "transform_deliveries.jsonl", {
            **delivery,
            "pipeline_id": pipeline.get("id"),
            "pipeline_code": pipeline.get("工序编号"),
        })
    persist.append_jsonl("reports", "transform_reruns.jsonl", {
        "pipeline_id": pipeline.get("id"),
        "sample_id": sample_id,
        "checklist_version": checklist.get("版本"),
        "rerun_scope": scope,
        "timestamp": persist.now_iso(),
    })
    return result


def preview_response(
    pipeline: dict[str, Any],
    checklist: dict[str, Any],
    preview: dict[str, Any],
) -> dict[str, Any]:
    return {
        "ok": True,
        "message": "转换预演完成",
        "sample_id": checklist.get("样本编号"),
        "样本编号": checklist.get("样本编号"),
        "template_version": checklist.get("模板版本"),
        "模板版本": checklist.get("模板版本"),
        "version": checklist.get("版本"),
        "清单版本": checklist.get("版本"),
        "checklist": checklist,
        "清单": checklist,
        "fields": preview.get("字段执行序列", []),
        "字段执行序列": preview.get("字段执行序列", []),
        "order": preview.get("执行次序", []),
        "执行次序": preview.get("执行次序", []),
        "computable": preview.get("可计算字段", {}),
        "可计算字段": preview.get("可计算字段", {}),
        "formal_values": checklist.get("正式值", {}),
        "正式值": checklist.get("正式值", {}),
        "blocked": preview.get("阻塞字段", {}),
        "阻塞依赖": preview.get("阻塞字段", {}),
        "blocked_fields": sorted(preview.get("阻塞字段", {}).keys()),
        "阻塞字段": preview.get("阻塞字段", {}),
        "branch_blocked": preview.get("阻塞依赖", {}),
        "分支阻塞依赖": preview.get("阻塞依赖", {}),
        "全部阻塞依赖": preview.get("阻塞依赖", {}),
        "rerun_scope": checklist.get("重跑范围", []),
        "重跑范围": checklist.get("重跑范围", []),
        "cyclic_fields": preview.get("成环字段", []),
        "成环字段": preview.get("成环字段", []),
    }


def result_response(
    pipeline: dict[str, Any],
    checklist: dict[str, Any],
    result: dict[str, Any],
) -> dict[str, Any]:
    pipeline_state = state_store.get_pipeline_state(
        int(pipeline.get("id", 0)), _text(pipeline.get("工序编号"))
    )
    return {
        "ok": True,
        "message": "正式转换完成",
        "result": result,
        "结果": result,
        "checklist": checklist,
        "清单": checklist,
        "checklist_version": checklist.get("版本"),
        "清单版本": checklist.get("版本"),
        "template_version": result.get("模板版本", ""),
        "模板版本": result.get("模板版本", ""),
        "sample_id": result.get("样本编号"),
        "样本编号": result.get("样本编号"),
        "fields": result.get("字段值", {}),
        "字段值": result.get("字段值", {}),
        "formal_values": result.get("正式值", {}),
        "正式值": result.get("正式值", {}),
        "delivered": result.get("本次交付", []),
        "本次交付": result.get("本次交付", []),
        "blocked": result.get("阻塞依赖", {}),
        "阻塞依赖": result.get("阻塞依赖", {}),
        "rerun_scope": result.get("重跑范围", []),
        "重跑范围": result.get("重跑范围", []),
        "deliveries": [
            item for item in state_store.active_deliveries(pipeline_state)
            if _text(item.get("样本编号")) == _text(result.get("样本编号"))
        ],
        "交付记录": result.get("交付记录", []),
    }
