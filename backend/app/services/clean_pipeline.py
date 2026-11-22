"""清洗管道领域服务：数据清洗编排，消费data_extract输出，驱动transform_rule和quality_guard。

启用工序时同步生成并提交「字段转换执行清单」，正式转换只消费这份清单：
按依赖次序预演真实采集行，可交付字段、阻塞依赖和重跑范围分开持久化。
"""
from __future__ import annotations

from typing import Any

import copy

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app.services import transform_engine as engine
from app.services import transform_state as transform_store
from app import persistence as persist


def _trigger_transform_on_enable(entry: dict[str, Any]) -> None:
    """清洗工序启用时用真实采集行预演关联转换规则，并提交执行清单。"""
    pipeline_id = entry.get("工序编号", "")
    transforms = engine.collect_rules(pipeline_id, entry.get("id"))
    if not transforms:
        persist.append_jsonl("reports", "pipeline_transform_check.jsonl", {
            "pipeline_id": entry.get("id"),
            "pipeline_code": pipeline_id,
            "linked_transforms": 0,
            "transform_ready": 0,
            "sample_id": "",
            "checklist_id": "",
            "deliverable_fields": [],
            "blocked_fields": [],
            "timestamp": persist.now_iso(),
        })
        return

    prepared = entry.get("_transform_request") or {}
    sample = prepared.get("sample")
    rule_ids = prepared.get("rule_ids")
    if rule_ids:
        transforms = engine.collect_rules(pipeline_id, entry.get("id"), rule_ids)

    try:
        checklist, preview, _state = engine.prepare_checklist(
            entry, prepared, sample=sample,
            rules=transforms,
            rerun=prepared.get("rerun"),
        )
    except ValueError as exc:
        persist.append_jsonl("reports", "pipeline_transform_check.jsonl", {
            "pipeline_id": entry.get("id"),
            "pipeline_code": pipeline_id,
            "linked_transforms": len(transforms),
            "transform_ready": sum(1 for t in transforms if t.get("status") == "已生效"),
            "error": str(exc),
            "timestamp": persist.now_iso(),
        })
        return

    result = engine.formal_transform(entry, checklist)
    persist.append_jsonl("reports", "pipeline_transform_check.jsonl", {
        "pipeline_id": entry.get("id"),
        "pipeline_code": pipeline_id,
        "linked_transforms": len(transforms),
        "transform_ready": sum(1 for t in transforms if t.get("status") == "已生效"),
        "sample_id": checklist.get("样本编号"),
        "template_version": checklist.get("模板版本"),
        "checklist_id": checklist.get("清单编号"),
        "checklist_version": checklist.get("版本"),
        "result_id": result.get("结果编号"),
        "deliverable_fields": list(result.get("字段值", {}).keys()),
        "blocked_fields": sorted(result.get("阻塞依赖", {}).keys()),
        "rerun_scope": result.get("重跑范围", []),
        "timestamp": persist.now_iso(),
    })

register_hook("clean_pipeline", "启用工序", _trigger_transform_on_enable)


class CleanPipelineService(BaseModuleService):
    MODULE = "clean_pipeline"
    ENTITY = "清洗工序"
    OPERATION_TERM = "清洗"
    LIST_FIELDS = ["工序编号", "输入源", "清洗规则", "处理顺序", "脏数据策略", "异常阈值", "采样比例", "工序版本"]
    REQUIRED_FIELDS = ["工序编号", "输入源", "清洗规则"]
    STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启用工序": "已启用", "停用工序": "已停用"}
    NEGATIVE_ACTIONS = ["停用工序"]

    def run_action(
        self,
        entry_id: int,
        action: str,
        payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """动作入口：启用工序时可携带样本和清单参数，供预演/正式转换消费。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        body = dict(payload or {})
        if action in {"提交转换预演", "转换预演", "字段转换预演", "预演"}:
            return self._preview(entry, body)
        if action in {"正式转换", "执行转换", "字段转换", "提交清单并转换"}:
            return self._formal(entry, body)
        if action in {"提交转换清单", "提交字段转换执行清单"}:
            return self._commit_checklist(entry, body)
        if action in {"启用工序", "启动清洗", "启动工序", "开始清洗"}:
            action = "启用工序"
            request: dict[str, Any] = {}
            for key in ("sample", "sample_id", "sample_row", "采集行", "采集样本",
                        "values", "字段值", "样本值", "sample_values", "fields",
                        "rule_ids", "rules", "rerun", "rerun_scope", "重跑范围",
                        "previous_version", "上一清单版本", "checklist_id",
                        "checklist", "checklist_version", "清单版本", "模板版本",
                        "template_version"):
                if key in body:
                    request[key] = body[key]
            entry["_transform_request"] = request
            try:
                return super().run_action(entry_id, action, body)
            finally:
                entry.pop("_transform_request", None)
                state = transform_store.get_pipeline_state(entry_id, str(entry.get("工序编号", "")))
                checklist = transform_store.find_checklist(
                    state, checklist_id=state.get("last_checklist_id"))
                if checklist:
                    entry["转换清单"] = checklist
                    entry["字段转换执行清单"] = checklist
                result = transform_store.latest_result(state)
                if result:
                    entry["转换结果"] = engine.result_response(entry, checklist or {}, result)
                    entry["字段转换结果"] = entry["转换结果"]
        return super().run_action(entry_id, action, body)

    def _preview(self, entry: dict[str, Any], body: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """转换预演视图：只读，可为多条候选规则展示按执行次序排列的字段。"""
        body = {**body, "dry_run": True}
        prepared: dict[str, Any] = {}
        if isinstance(body.get("checklist"), dict):
            body.setdefault("sample", {
                "样本编号": body["checklist"].get("样本编号"),
                "字段值": body["checklist"].get("样本值", {}),
                "模板版本": body["checklist"].get("模板版本", ""),
            })
        if isinstance(body.get("sample"), dict):
            prepared["sample"] = engine.normalize_sample(body["sample"])
        try:
            checklist, preview, _state = engine.prepare_checklist(
                entry, body, sample=prepared.get("sample"),
                rerun=body.get("rerun") if isinstance(body.get("rerun"), bool) else None,
                persist_checklist=False,
            )
        except ValueError as exc:
            return None, str(exc)
        response = engine.preview_response(entry, checklist, preview)
        entry["last_preview"] = response
        entry["转换预演"] = response
        return entry, "转换预演完成"

    def _commit_checklist(self, entry: dict[str, Any], body: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """只提交清单（预演并冻结），不写正式值。"""
        try:
            checklist, preview, _state = engine.prepare_checklist(entry, body)
        except ValueError as exc:
            return None, str(exc)
        entry["转换清单"] = checklist
        entry["字段转换执行清单"] = checklist
        entry["转换预演"] = engine.preview_response(entry, checklist, preview)
        return entry, "字段转换执行清单已提交"

    def _formal(self, entry: dict[str, Any], body: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """正式转换：消费清单（清单冻结样本身份、规则版本和重跑范围）。"""
        state = transform_store.get_pipeline_state(
            int(entry.get("id", 0)), str(entry.get("工序编号", "")))
        checklist = None
        if isinstance(body.get("checklist"), dict):
            checklist = body["checklist"]
        elif body.get("checklist_id"):
            checklist = transform_store.find_checklist(
                state, checklist_id=str(body["checklist_id"]))
        elif body.get("checklist_version") or body.get("清单版本"):
            checklist = transform_store.find_checklist(
                state, version=body.get("checklist_version") or body.get("清单版本"))
        elif body.get("sample") or body.get("sample_id") or body.get("values"):
            prepared: dict[str, Any] = {}
            if isinstance(body.get("sample"), dict):
                prepared["sample"] = engine.normalize_sample(body["sample"])
            elif body.get("sample_id"):
                prepared["sample_id"] = body["sample_id"]
            try:
                checklist, _preview, _state = engine.prepare_checklist(
                    entry, {**body, **prepared},
                    sample=prepared.get("sample"),
                    rerun=body.get("rerun") if isinstance(body.get("rerun"), bool) else None,
                )
            except ValueError as exc:
                return None, str(exc)
        else:
            checklist = transform_store.find_checklist(
                state, checklist_id=state.get("last_checklist_id"))

        if checklist is None:
            try:
                checklist, _preview, _state = engine.prepare_checklist(
                    entry, body,
                    rerun=body.get("rerun") if isinstance(body.get("rerun"), bool) else None,
                )
            except ValueError as exc:
                return None, str(exc)
        elif not self._checklist_is_committed(state, checklist):
            # 调用方传的是预演清单：正式转换前先提交为可回查的正式版本
            proposed_sample = {
                "样本编号": checklist.get("样本编号"),
                "字段值": checklist.get("样本值", {}),
                "模板版本": checklist.get("模板版本", ""),
            }
            try:
                checklist, _preview, _state = engine.prepare_checklist(
                    entry, body, sample=proposed_sample,
                    rerun=body.get("rerun") if isinstance(body.get("rerun"), bool) else None,
                )
            except ValueError as exc:
                return None, str(exc)

        # 清单声明的重跑范围可以由调用方收窄，但不会扩大
        scope = body.get("rerun_scope") or body.get("重跑范围") or body.get("fields")
        if scope:
            declared = set(checklist.get("重跑范围") or [])
            requested = [str(item) for item in scope] if isinstance(scope, (list, tuple)) else [str(scope)]
            narrowed = [item for item in requested if not declared or item in declared]
            checklist = {**checklist, "重跑范围": narrowed or list(declared)}

        sample = None
        sample_id = str(checklist.get("样本编号") or "")
        if sample_id:
            sample = transform_store.get_sample(sample_id)
        result = engine.formal_transform(entry, checklist, sample=sample)
        response = engine.result_response(entry, checklist, result)
        entry["last_transform_result"] = response
        entry["转换结果"] = response
        entry["字段转换结果"] = response
        entry["转换清单"] = checklist
        entry["字段转换执行清单"] = checklist
        return entry, "正式转换完成"

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """读取工序时附带最近的执行清单和正式转换结果，便于直接核对出数。"""
        found = super().get_entry(entry_id)
        if found is None:
            return None
        entry = copy.deepcopy(found)
        state = transform_store.get_pipeline_state(
            int(entry.get("id", 0)), str(entry.get("工序编号", "")))
        checklist = transform_store.find_checklist(
            state, checklist_id=state.get("last_checklist_id"))
        if checklist:
            entry["转换清单"] = copy.deepcopy(checklist)
            entry["字段转换执行清单"] = copy.deepcopy(checklist)
        result = transform_store.latest_result(state)
        if result:
            response = engine.result_response(entry, checklist or {}, result)
            entry["转换结果"] = response
            entry["字段转换结果"] = response
        return entry

    def get_linked_transforms(self, pipeline_code: str) -> list[dict[str, Any]]:
        """查询某工序关联的所有转换规则。"""
        return [t for t in store.rows("transform_rule") if t.get("关联工序") == pipeline_code]

    def preview_transform(
        self,
        pipeline_code: str,
        body: dict[str, Any] | None = None,
        pipeline_id: int | None = None,
    ) -> dict[str, Any]:
        """按工序编号做转换预演（供路由直接调用）。"""
        body = body or {}
        entry = self._find_pipeline(pipeline_code, pipeline_id)
        if entry is None:
            raise ValueError(f"清洗工序 {pipeline_code} 不存在或已归档")
        prepared: dict[str, Any] = {}
        if isinstance(body.get("sample"), dict):
            prepared["sample"] = engine.normalize_sample(body["sample"])
        checklist, preview, _state = engine.prepare_checklist(
            entry, body, sample=prepared.get("sample"),
            rerun=body.get("rerun") if isinstance(body.get("rerun"), bool) else None,
            persist_checklist=False,
        )
        return engine.preview_response(entry, checklist, preview)

    def formal_transform(
        self,
        pipeline_code: str,
        body: dict[str, Any] | None = None,
        pipeline_id: int | None = None,
    ) -> dict[str, Any]:
        """按工序编号执行正式转换（供路由直接调用）。"""
        body = body or {}
        entry = self._find_pipeline(pipeline_code, pipeline_id)
        if entry is None:
            raise ValueError(f"清洗工序 {pipeline_code} 不存在或已归档")
        _entry, message = self._formal(entry, body)
        if "last_transform_result" not in entry:
            raise ValueError(message)
        return entry["last_transform_result"]

    def get_checklist(
        self,
        pipeline_code: str,
        version: str | int | None = None,
        checklist_id: str | None = None,
        pipeline_id: int | None = None,
    ) -> dict[str, Any] | None:
        entry = self._find_pipeline(pipeline_code, pipeline_id)
        if entry is None:
            return None
        state = transform_store.get_pipeline_state(
            int(entry.get("id", 0)), str(entry.get("工序编号", "")))
        return transform_store.find_checklist(state, version=version, checklist_id=checklist_id)

    def list_checklists(
        self,
        pipeline_code: str,
        pipeline_id: int | None = None,
    ) -> list[dict[str, Any]]:
        entry = self._find_pipeline(pipeline_code, pipeline_id)
        if entry is None:
            return []
        state = transform_store.get_pipeline_state(
            int(entry.get("id", 0)), str(entry.get("工序编号", "")))
        return [dict(item) for item in state.get("checklists", [])]

    def get_transform_result(
        self,
        pipeline_code: str,
        pipeline_id: int | None = None,
        version: str | int | None = None,
        checklist_id: str | None = None,
    ) -> dict[str, Any] | None:
        entry = self._find_pipeline(pipeline_code, pipeline_id)
        if entry is None:
            return None
        state = transform_store.get_pipeline_state(
            int(entry.get("id", 0)), str(entry.get("工序编号", "")))
        results = state.get("results", [])
        result = None
        if version is not None or checklist_id:
            for item in results:
                if checklist_id and str(item.get("清单编号")) == str(checklist_id):
                    result = item
                    break
                if version is not None and (
                    str(item.get("清单版本")) == str(version)
                    or str(item.get("清单版本")) == f"v{version}"
                ):
                    result = item
                    break
            if result is None:
                return None
        else:
            result = transform_store.latest_result(state)
        if result is None:
            return None
        checklist = transform_store.find_checklist(
            state, checklist_id=result.get("清单编号")) or {}
        return engine.result_response(entry, checklist, result)

    def list_transform_results(
        self,
        pipeline_code: str,
        pipeline_id: int | None = None,
    ) -> list[dict[str, Any]]:
        entry = self._find_pipeline(pipeline_code, pipeline_id)
        if entry is None:
            return []
        state = transform_store.get_pipeline_state(
            int(entry.get("id", 0)), str(entry.get("工序编号", "")))
        return [dict(item) for item in state.get("results", [])]

    # 兼容不同调用方的命名习惯
    preview_sample = preview_transform
    transform_preview = preview_transform
    build_checklist = preview_transform
    run_formal_transform = formal_transform
    execute_transform = formal_transform
    consume_checklist = formal_transform
    execution_checklist = get_checklist

    def get_sample_report(self, sample_id: str) -> dict[str, Any] | None:
        return transform_store.get_sample(sample_id)

    def _checklist_is_committed(self, state: dict[str, Any], checklist: dict[str, Any]) -> bool:
        identifier = str(checklist.get("清单编号") or "")
        if not identifier:
            return False
        for item in state.get("checklists", []):
            if str(item.get("清单编号")) == identifier:
                return True
        return False

    def _find_pipeline(self, pipeline_code: str, pipeline_id: int | None = None) -> dict[str, Any] | None:
        if pipeline_id is not None:
            found = store.find(self.MODULE, int(pipeline_id))
            if found is not None:
                return found
        code = str(pipeline_code or "").strip()
        if not code:
            return None
        for entry in store.rows(self.MODULE):
            if str(entry.get("工序编号", "")) == code:
                return entry
        try:
            return store.find(self.MODULE, int(code))
        except (TypeError, ValueError):
            return None

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        pipelines = store.rows(self.MODULE)
        # 读取关联的质量报告
        reports = persist.read_jsonl("reports", "quality_report.jsonl")
        base["quality_checks"] = len(reports)
        # 关联的归一化规则
        norms = store.rows("field_normalize")
        base["linked_normalize_rules"] = len(norms)
        # 转换预演/正式转换结果
        transform_reports = persist.read_jsonl("reports", "transform_results.jsonl")
        base["transform_results"] = len(transform_reports)
        base["blocked_fields"] = sorted({
            field
            for item in transform_reports
            for field in (item.get("blocked_dependencies") or {})
        })
        base["samples_registered"] = len(transform_store.list_samples())
        base["checklists_committed"] = sum(
            len(transform_store.get_pipeline_state(int(p.get("id", 0)), str(p.get("工序编号", "")))
                .get("checklists", []))
            for p in pipelines
        )
        return base
