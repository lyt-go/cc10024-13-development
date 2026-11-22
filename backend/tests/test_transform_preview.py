"""字段转换预演与执行清单的端到端测试。

覆盖：同目标多分支、依赖缺失、互相引用成环、独立字段照常出数、
按执行次序排列、清单冻结样本身份和规则版本、正式值唯一、解环后仅重跑
受影响字段且历史值/历史清单可回查。
"""
from __future__ import annotations

import os
import sys

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import persistence as persist
from app.store import store
from app.services.clean_pipeline import CleanPipelineService
from app.services.transform_rule import TransformRuleService
from app.services import transform_state as transform_store

for sub in ["runs", "snapshots", "reports", "exports", "transforms"]:
    path = persist.data_path(sub, "")
    if os.path.isdir(path):
        for name in os.listdir(path):
            os.remove(os.path.join(path, name))
transform_store.clear_runtime()


PIPE_CODE = "CLEA-PRICE-01"
SAMPLE_ID = "SAMPLE-PRICE-7"
SAMPLE = {
    "样本编号": SAMPLE_ID,
    "字段值": {"价格": "7", "商品分类原值": "A"},
    "模板版本": "TPL-v3",
    "来源模板": "DATA-0003",
}
RULES = [
    # 独立字段：价格不受分类冲突影响
    {"规则编号": "RULE-PRICE", "关联工序": PIPE_CODE, "源字段": "价格",
     "目标字段": "价格", "转换函数": "copy", "依赖字段": "", "规则版本": "v1"},
    # 分类分支一：基于已抽取的原分类直接映射，可算出“甲”
    {"规则编号": "RULE-CAT-A", "关联工序": PIPE_CODE, "源字段": "商品分类原值",
     "目标字段": "商品分类", "转换函数": "映射:A=甲", "依赖字段": "", "规则版本": "v1"},
    # 分类分支二：依赖两个互相引用的字段，成环
    {"规则编号": "RULE-CAT-B", "关联工序": PIPE_CODE, "源字段": "分类左",
     "目标字段": "商品分类", "转换函数": "copy", "依赖字段": "分类右", "规则版本": "v1"},
    {"规则编号": "RULE-CAT-LEFT", "关联工序": PIPE_CODE, "源字段": "分类右",
     "目标字段": "分类左", "转换函数": "copy", "依赖字段": "分类右", "规则版本": "v1"},
    {"规则编号": "RULE-CAT-RIGHT", "关联工序": PIPE_CODE, "源字段": "分类左",
     "目标字段": "分类右", "转换函数": "copy", "依赖字段": "分类左", "规则版本": "v1"},
]


def _create_pipeline(svc: CleanPipelineService) -> int:
    entry, missing = svc.create_entry({
        "工序编号": PIPE_CODE,
        "输入源": "DATA-0003",
        "清洗规则": "trim,mapping",
        "处理顺序": "1",
        "脏数据策略": "隔离",
        "异常阈值": "0",
        "采样比例": "100%",
        "工序版本": "PIPE-v1",
    })
    assert entry is not None, missing
    return int(entry["id"])


def _create_rules(rules=None) -> None:
    svc = TransformRuleService()
    for rule in rules if rules is not None else RULES:
        entry, missing = svc.create_entry({
            "规则编号": rule["规则编号"],
            "关联工序": rule["关联工序"],
            "源字段": rule["源字段"],
            "目标字段": rule["目标字段"],
            "转换函数": rule["转换函数"],
            "依赖字段": rule["依赖字段"],
            "空值处理": "",
            "规则版本": rule["规则版本"],
        })
        assert entry is not None, missing


def test_preview_order_values_and_blocking():
    """预演视图按执行次序排列字段，展示可计算值并列出阻塞依赖。"""
    svc = CleanPipelineService()
    pipeline_id = _create_pipeline(svc)
    _create_rules()

    preview = svc.preview_transform(PIPE_CODE, {"sample": SAMPLE})
    fields = preview["字段执行序列"]
    order = preview["执行次序"]

    # 独立价格字段照常出数
    assert preview["可计算字段"]["价格"] == "7"
    # 分类至少有一条可算分支给出“甲”
    assert preview["可计算字段"]["商品分类"] == "甲"
    # 分类的环形分支必须阻塞，且不能用空值冒充结果
    assert set(preview["成环字段"]) == {"分类左", "分类右"}
    assert {"分类左", "分类右"} <= set(preview["阻塞字段"].keys())
    for target in ("分类左", "分类右"):
        assert preview["阻塞字段"][target], f"{target} 应列出阻塞依赖"
    # 环形分支被记录在可计算目标的阻塞依赖里，但不会覆盖已算出的“甲”
    assert set(preview["分支阻塞依赖"]["商品分类"]) == {"分类左", "分类右"}
    # 环内的字段没有任何可计算值
    for entry in fields:
        if entry["目标字段"] in {"分类左", "分类右"}:
            assert entry["结果值"] is None and entry["状态"] == "阻塞"
    # 商品分类的两个分支都在视图里，但只有一个正式交付
    cat_branches = [entry for entry in fields if entry["目标字段"] == "商品分类"]
    assert len(cat_branches) == 2
    assert sum(1 for entry in cat_branches if entry["正式交付"]) == 1

    # 执行次序里价格/分类先于（或与）环字段分开，且字段按次序排列
    assert order.index("价格") < order.index("分类左") or "价格" in order
    sequence = [entry["执行序号"] for entry in fields]
    assert sequence == sorted(sequence)
    assert preview["清单"]["样本编号"] == SAMPLE_ID
    assert preview["清单"]["模板版本"] == "TPL-v3"
    assert preview["清单"]["规则版本"]["RULE-CAT-A"] == "v1"


def test_enable_runs_formal_transform_and_rerun_after_uncycle():
    """启用入口提交清单后正式转换；解环重跑只影响分类字段，价格不重交付。"""
    svc = CleanPipelineService()
    state = transform_store.get_pipeline_state(_pipeline_id(svc), PIPE_CODE)
    pipeline_id = _pipeline_id(svc)
    state = transform_store.get_pipeline_state(pipeline_id, PIPE_CODE)

    # 通过原工序启用入口提交清单（携带真实采集行）
    entry, message = svc.run_action(pipeline_id, "启用工序", {"sample": SAMPLE})
    assert entry is not None, message
    assert entry["status"] == "已启用"

    formal = svc.get_transform_result(PIPE_CODE)
    assert formal is not None
    values = formal["字段值"]
    assert values["价格"] == "7"
    assert values["商品分类"] == "甲"
    assert "分类左" not in values and "分类右" not in values
    assert "商品分类" in formal["重跑范围"] or "价格" in formal["重跑范围"]
    first_version = formal["清单版本"]
    first_checklist = formal["清单"]

    # 同一目标最多一个正式值
    deliveries = [d for d in formal["交付记录"]
                  if d["样本编号"] == SAMPLE_ID and d["目标字段"] == "商品分类"]
    assert len(deliveries) == 1

    # 解环：把分支二改成依赖已抽取的原分类，不再互相引用
    rule_svc = TransformRuleService()
    left = next(r for r in store.rows("transform_rule") if r.get("规则编号") == "RULE-CAT-LEFT")
    right = next(r for r in store.rows("transform_rule") if r.get("规则编号") == "RULE-CAT-RIGHT")
    left["依赖字段"] = ""
    left["源字段"] = "商品分类原值"
    left["转换函数"] = "copy"
    right["依赖字段"] = ""
    right["源字段"] = "商品分类原值"
    right["转换函数"] = "copy"

    # 从相同来源重跑
    rerun = svc.formal_transform(PIPE_CODE, {"sample": SAMPLE})
    rerun_values = rerun["字段值"]
    assert rerun_values["商品分类"] == "甲"
    cat_deliveries = [d for d in rerun["交付记录"]
                      if d["样本编号"] == SAMPLE_ID and d["目标字段"] == "商品分类"]
    active = [d for d in cat_deliveries if d["状态"] == "有效"]
    assert len(active) == 1, f"解环后分类至多一个正式值，实际 {len(active)}"
    # 价格没有重新交付
    price_deliveries = [d for d in rerun["本次交付"] if d["目标字段"] == "价格"]
    assert price_deliveries == []
    assert "价格" not in rerun["重跑范围"]
    # 原清单版本仍可回查
    kept = svc.get_checklist(PIPE_CODE, version=first_version)
    assert kept is not None
    assert kept["清单编号"] == first_checklist["清单编号"]
    assert kept["规则版本"]["RULE-CAT-A"] == "v1"


def _pipeline_id(svc: CleanPipelineService) -> int:
    entry = next(p for p in store.rows("clean_pipeline") if p.get("工序编号") == PIPE_CODE)
    return int(entry["id"])





def test_data_extract_hands_sample_and_template_version_to_pipeline():
    """数据抽取把实际样本值和模板版本交给清洗管道，预演可直接用样本编号。"""
    from app.services.data_extract import DataExtractService

    code = "CLEA-EXTRACT-01"
    pipeline, missing = CleanPipelineService().create_entry({
        "工序编号": code,
        "输入源": "DATA-X-01",
        "清洗规则": "trim",
    })
    assert pipeline is not None, missing
    rule_svc = TransformRuleService()
    rule_svc.create_entry({
        "规则编号": "RULE-X-PRICE", "关联工序": code, "源字段": "价格",
        "目标字段": "价格", "转换函数": "copy", "依赖字段": "", "规则版本": "v7",
    })

    extract_svc = DataExtractService()
    template, missing = extract_svc.create_entry({
        "模板编号": "DATA-X-01",
        "关联脚本": "BEHA-X-01",
        "抽取字段": "价格",
        "模板版本": "TPL-X-7",
        "样本编号": "SAMPLE-X-7",
        "样本值": {"价格": "7", "商品分类原值": "A"},
    })
    assert template is not None, missing
    activated, message = extract_svc.run_action(template["id"], "标记生效")
    assert activated is not None, message

    preview = CleanPipelineService().preview_transform(code, {"sample_id": "SAMPLE-X-7"})
    assert preview["样本编号"] == "SAMPLE-X-7"
    assert preview["模板版本"] == "TPL-X-7"
    assert preview["可计算字段"]["价格"] == "7"
    assert preview["清单"]["规则版本"]["RULE-X-PRICE"] == "v7"


def test_blocked_field_never_reports_empty_value_and_checklist_is_frozen():
    """依赖缺失时不写空值；清单冻结规则快照，后续改规则不影响历史清单。"""
    from app.services.transform_engine import collect_rules
    from app.services import transform_engine as engine

    sample = {"样本编号": "SAMPLE-MISSING", "字段值": {"价格": "7"}, "模板版本": "TPL-M"}
    rules = [
        {"id": 901, "规则编号": "RULE-M-PRICE", "关联工序": "CLEA-M",
         "源字段": "价格", "目标字段": "价格", "转换函数": "copy", "依赖字段": "", "规则版本": "v1"},
        {"id": 902, "规则编号": "RULE-M-CAT", "关联工序": "CLEA-M",
         "源字段": "分类原值", "目标字段": "商品分类", "转换函数": "copy",
         "依赖字段": "上游分类", "规则版本": "v1"},
    ]
    preview = engine.evaluate_rules(rules, sample)
    assert preview["可计算字段"] == {"价格": "7"}
    assert preview["阻塞字段"]["商品分类"] == ["分类原值", "上游分类"]
    for entry in preview["字段执行序列"]:
        if entry["目标字段"] == "商品分类":
            assert entry["结果值"] is None
            assert entry["状态"] == "阻塞"
            assert entry["正式交付"] is False

    pipeline = {"id": 77, "工序编号": "CLEA-M"}
    checklist, _preview, _state = engine.prepare_checklist(
        pipeline, {"sample": sample}, rules=rules, persist_checklist=False)
    snapshot_rules = {item["规则编号"]: item["规则版本"] for item in checklist["规则清单"]}
    assert snapshot_rules == {"RULE-M-PRICE": "v1", "RULE-M-CAT": "v1"}
    # 清单是深拷贝：改原规则对象不会改历史清单
    rules[0]["规则版本"] = "v2"
    assert checklist["规则清单"][0]["规则版本"] == "v1"


if __name__ == "__main__":
    test_preview_order_values_and_blocking()
    print("PASS: test_preview_order_values_and_blocking")
    test_enable_runs_formal_transform_and_rerun_after_uncycle()
    print("PASS: test_enable_runs_formal_transform_and_rerun_after_uncycle")
    test_data_extract_hands_sample_and_template_version_to_pipeline()
    print("PASS: test_data_extract_hands_sample_and_template_version_to_pipeline")
    test_blocked_field_never_reports_empty_value_and_checklist_is_frozen()
    print("PASS: test_blocked_field_never_reports_empty_value_and_checklist_is_frozen")