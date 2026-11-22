"""字段转换运行态仓库：样本、预演清单、正式结果和交付记录。

数据在内存里按工序聚合，同时落到 ``data/transforms/``，进程重启后仍可回查
原先的清单版本和已经交付的字段。清单一旦生成就不再改写，重跑会生成新的
清单版本，避免覆盖历史。
"""
from __future__ import annotations

import copy
import os
import threading
from typing import Any

from app import persistence as persist

_LOCK = threading.RLock()
_SAMPLES: dict[str, dict[str, Any]] = {}
_PIPELINES: dict[str, dict[str, Any]] = {}
_LOADED = False
_DISK_STAMP: tuple[Any, ...] = ()


def _pipeline_key(pipeline_id: int | str) -> str:
    return str(pipeline_id)


def _transforms_dir() -> str:
    return os.path.dirname(persist.data_path("transforms", "stamp.json"))


def _disk_stamp() -> tuple[Any, ...]:
    """数据目录指纹：文件被外部清空/替换时重新加载，避免读到旧运行态。"""
    path = _transforms_dir()
    if not os.path.isdir(path):
        return ()
    entries: list[tuple[str, int]] = []
    for name in sorted(os.listdir(path)):
        try:
            entries.append((name, os.path.getmtime(os.path.join(path, name))))
        except OSError:
            continue
    return tuple(entries)


def _ensure_loaded(force: bool = False) -> None:
    """懒加载磁盘上的样本和工序转换状态；磁盘变化时重新加载。"""
    global _LOADED, _DISK_STAMP
    stamp = _disk_stamp()
    if _LOADED and not force and stamp == _DISK_STAMP:
        return
    with _LOCK:
        stamp = _disk_stamp()
        if _LOADED and not force and stamp == _DISK_STAMP:
            return
        _SAMPLES.clear()
        _PIPELINES.clear()
        samples = persist.read_json("transforms", "samples.json") or {}
        if isinstance(samples, dict):
            for key, value in samples.items():
                if isinstance(value, dict):
                    _SAMPLES[str(key)] = value
        for name in persist.list_files("transforms", ".json"):
            if not name.startswith("pipeline_"):
                continue
            data = persist.read_json("transforms", name)
            if isinstance(data, dict) and data.get("pipeline_id") is not None:
                _PIPELINES[_pipeline_key(data["pipeline_id"])] = data
        _DISK_STAMP = stamp
        _LOADED = True


def _default_pipeline_state(pipeline_id: int, pipeline_code: str = "") -> dict[str, Any]:
    return {
        "pipeline_id": pipeline_id,
        "pipeline_code": pipeline_code,
        "checklists": [],
        "results": [],
        "deliveries": [],
        "rerun_scope": [],
        "next_version": 1,
        "created_at": persist.now_iso(),
        "updated_at": persist.now_iso(),
    }


def get_pipeline_state(pipeline_id: int, pipeline_code: str = "") -> dict[str, Any]:
    """取出工序的转换状态；不存在时创建空状态。"""
    _ensure_loaded()
    key = _pipeline_key(pipeline_id)
    with _LOCK:
        state = _PIPELINES.get(key)
        if state is None:
            state = _default_pipeline_state(pipeline_id, pipeline_code)
            _PIPELINES[key] = state
        if pipeline_code and not state.get("pipeline_code"):
            state["pipeline_code"] = pipeline_code
        return state


def save_pipeline_state(state: dict[str, Any]) -> None:
    """把工序状态整体写回内存和磁盘。"""
    _ensure_loaded()
    with _LOCK:
        state["updated_at"] = persist.now_iso()
        key = _pipeline_key(state.get("pipeline_id", ""))
        _PIPELINES[key] = state
        persist.write_json("transforms", f"pipeline_{key}.json", copy.deepcopy(state))
        global _DISK_STAMP
        _DISK_STAMP = _disk_stamp()


def save_sample(record: dict[str, Any]) -> dict[str, Any]:
    """登记一条真实采集样本，以样本编号为身份。"""
    _ensure_loaded()
    with _LOCK:
        sample_id = str(record.get("样本编号") or record.get("sample_id") or "").strip()
        if not sample_id:
            sample_id = f"SAMPLE-{len(_SAMPLES) + 1:04d}"
            record["样本编号"] = sample_id
        record = copy.deepcopy(record)
        record["sample_id"] = sample_id
        if not str(record.get("创建时间") or "").strip():
            record["创建时间"] = persist.now_iso()
        _SAMPLES[sample_id] = record
        persist.write_json("transforms", "samples.json", copy.deepcopy(_SAMPLES))
        global _DISK_STAMP
        _DISK_STAMP = _disk_stamp()
        return copy.deepcopy(record)


def get_sample(sample_id: str | None) -> dict[str, Any] | None:
    _ensure_loaded()
    if not sample_id:
        return None
    with _LOCK:
        record = _SAMPLES.get(str(sample_id))
        return copy.deepcopy(record) if record else None


def list_samples() -> list[dict[str, Any]]:
    _ensure_loaded()
    with _LOCK:
        return [copy.deepcopy(item) for item in _SAMPLES.values()]


def _pipeline_links(record: dict[str, Any]) -> list[str]:
    raw = record.get("关联工序") or record.get("pipeline_code") or record.get("pipeline_codes") or []
    if isinstance(raw, str):
        return [part.strip() for part in raw.replace("，", ",").split(",") if part.strip()]
    if isinstance(raw, (list, tuple, set)):
        return [str(part).strip() for part in raw if str(part).strip()]
    return [str(raw).strip()] if str(raw).strip() else []


def latest_sample(pipeline_code: str = "", pipeline_id: int | None = None) -> dict[str, Any] | None:
    """寻找某工序最近一次交付的样本；没有工序匹配时退回全局最新样本。"""
    _ensure_loaded()
    with _LOCK:
        candidates = list(_SAMPLES.values())
        if not candidates:
            return None
        candidates.sort(key=lambda item: str(item.get("创建时间", "")), reverse=True)
        for record in candidates:
            links = _pipeline_links(record)
            if pipeline_code and pipeline_code in links:
                return copy.deepcopy(record)
            if pipeline_id is not None and str(pipeline_id) in links:
                return copy.deepcopy(record)
            source = str(record.get("来源模板", ""))
            if pipeline_code and source and source in pipeline_code:
                return copy.deepcopy(record)
        return copy.deepcopy(candidates[0])


def next_checklist_version(state: dict[str, Any]) -> tuple[int, str]:
    """分配下一个清单版本号，版本标签形如 ``v1``。"""
    with _LOCK:
        number = int(state.get("next_version") or 1)
        state["next_version"] = number + 1
        return number, f"v{number}"


def find_checklist(
    state: dict[str, Any],
    *,
    version: str | int | None = None,
    checklist_id: str | None = None,
) -> dict[str, Any] | None:
    """按版本号或清单编号读取历史清单（返回深拷贝，不允许调用方改写历史）。"""
    checklists = state.get("checklists", [])
    if not checklists:
        return None
    if checklist_id:
        for item in checklists:
            if str(item.get("清单编号")) == str(checklist_id):
                return copy.deepcopy(item)
    if version is not None:
        wanted = str(version).strip()
        if wanted and not wanted.lower().startswith("v"):
            wanted = f"v{wanted}"
        for item in checklists:
            if str(item.get("版本")) == wanted or str(item.get("版本号")) == str(version):
                return copy.deepcopy(item)
    return copy.deepcopy(checklists[-1])


def latest_result(state: dict[str, Any]) -> dict[str, Any] | None:
    results = state.get("results", [])
    return copy.deepcopy(results[-1]) if results else None


def active_deliveries(state: dict[str, Any]) -> list[dict[str, Any]]:
    """当前仍然有效的正式值，每个样本的每个目标字段最多一条。"""
    return [
        copy.deepcopy(item) for item in state.get("deliveries", [])
        if item.get("状态", "有效") == "有效"
    ]


def clear_runtime(clear_samples: bool = True) -> None:
    """测试辅助：清空进程内运行态（不删除磁盘文件）。"""
    global _LOADED, _DISK_STAMP
    with _LOCK:
        _PIPELINES.clear()
        if clear_samples:
            _SAMPLES.clear()
        _LOADED = False
        _DISK_STAMP = ()


def reset_state() -> None:
    """清空运行态并删除落盘文件，供测试或重建环境使用。"""
    global _LOADED, _DISK_STAMP
    with _LOCK:
        _PIPELINES.clear()
        _SAMPLES.clear()
        _LOADED = False
        _DISK_STAMP = ()
        for name in persist.list_files("transforms", ".json"):
            try:
                os.remove(persist.data_path("transforms", name))
            except OSError:
                continue
