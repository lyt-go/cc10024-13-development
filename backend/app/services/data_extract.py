"""数据抽取领域服务：消费behavior_script脚本和element_selector锚点，执行字段抽取。

抽取模板标记生效时，会把实际样本值和模板版本登记给清洗管道，让清洗侧的
转换预演直接拿到真实采集行，而不是只看模板上的字段名。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app.services import transform_state as transform_store
from app import persistence as persist


def _register_sample_to_pipeline(entry: dict[str, Any]) -> None:
    """抽取模板生效时把实际样本值、模板版本交给清洗管道。"""
    template_code = entry.get("模板编号", "")
    template_version = entry.get("模板版本", "")
    pipeline_rows = store.rows("clean_pipeline")
    script_id = str(entry.get("关联脚本", ""))
    explicit_codes = [
        str(code) for code in _as_list(entry.get("关联工序")) if str(code).strip()
    ]
    linked_pipelines = [
        p for p in pipeline_rows
        if script_id and script_id in str(p.get("输入源", ""))
    ]
    linked_codes = explicit_codes or [
        str(p.get("工序编号")) for p in linked_pipelines if p.get("工序编号")
    ]

    # 样本值优先取模板上携带的真实采集行，回退到抽取字段配置
    raw_values = (
        entry.get("样本值")
        or entry.get("字段值")
        or entry.get("抽取样本")
        or entry.get("实际样本值")
    )
    if isinstance(raw_values, dict):
        values = dict(raw_values)
    elif isinstance(raw_values, str) and raw_values.strip():
        values = {"原始值": raw_values.strip()}
    else:
        field_name = str(entry.get("抽取字段", "")).strip()
        default_value = entry.get("默认值")
        values = {field_name: default_value} if field_name and default_value not in (None, "") else {}

    sample_id = str(
        entry.get("样本编号") or entry.get("sample_id")
        or f"SAMPLE-{template_code}-{entry.get('id')}"
    )
    sample = {
        "样本编号": sample_id,
        "字段值": values,
        "模板版本": template_version,
        "来源模板": template_code,
        "关联脚本": script_id,
        "来源": "data_extract",
        "关联工序": linked_codes,
    }
    stored = transform_store.save_sample(sample)
    persist.append_jsonl("reports", "data_extract_samples.jsonl", {
        "template_id": entry.get("id"),
        "template_code": template_code,
        "template_version": template_version,
        "sample_id": stored.get("样本编号"),
        "field_names": list(values.keys()),
        "linked_pipelines": linked_codes,
        "timestamp": persist.now_iso(),
    })

register_hook("data_extract", "标记生效", _register_sample_to_pipeline)

_SAMPLE_KEYS = (
    "样本值", "字段值", "抽取样本", "实际样本值", "sample", "sample_id", "样本编号",
)


def _as_list(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        return list(raw)
    return [part.strip() for part in str(raw).replace("，", ",").split(",") if part.strip()]


class DataExtractService(BaseModuleService):
    MODULE = "data_extract"
    ENTITY = "抽取模板"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["模板编号", "关联脚本", "抽取字段", "提取方式", "数据处理", "输出类型", "默认值", "模板版本"]
    REQUIRED_FIELDS = ["模板编号", "关联脚本", "抽取字段"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用模板": "已停用"}
    NEGATIVE_ACTIONS = ["停用模板"]

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记抽取模板时保留实际样本值和样本身份，供清洗管道预演。"""
        entry, missing = super().create_entry(values)
        if entry is None:
            return entry, missing
        for key in _SAMPLE_KEYS + ("关联工序",):
            if values.get(key) not in (None, ""):
                entry[key] = values[key]
        return entry, missing

    def run_action(
        self,
        entry_id: int,
        action: str,
        payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """标记生效时可以把实际样本值和模板版本一并交给清洗管道。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        body = payload or {}
        for key in _SAMPLE_KEYS + ("关联工序", "模板版本"):
            if key in body and body[key] not in (None, ""):
                entry[key] = body[key]
        return super().run_action(entry_id, action, body)

    def extract_fields_for_script(self, script_id: str) -> list[dict[str, Any]]:
        """根据脚本编号查询所有关联的抽取模板并返回字段映射。"""
        templates = store.rows(self.MODULE)
        linked = [t for t in templates if t.get("关联脚本") == script_id and t.get("status") == "已生效"]
        return [{"field": t.get("抽取字段"), "method": t.get("提取方式"), "output_type": t.get("输出类型")} for t in linked]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        templates = store.rows(self.MODULE)
        # 关联的元素锚点统计
        selectors = store.rows("element_selector")
        base["templates_with_selectors"] = sum(
            1 for t in templates if any(
                s.get("关联脚本") == t.get("关联脚本") for s in selectors))
        # 关联的清洗管道
        pipelines = store.rows("clean_pipeline")
        base["templates_in_pipeline"] = sum(
            1 for t in templates if any(
                p.get("输入源") and t.get("关联脚本") in str(p.get("输入源", "")) for p in pipelines))
        # 已交给清洗管道的样本
        sample_reports = persist.read_jsonl("reports", "data_extract_samples.jsonl")
        base["samples_handed_to_pipeline"] = len(sample_reports)
        return base
