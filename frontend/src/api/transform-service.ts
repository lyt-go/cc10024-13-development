import { request } from './client'

export type TransformFieldEntry = {
  执行序号: number
  目标字段: string
  规则编号: string
  规则版本: string
  源字段: string
  依赖字段: string
  转换函数: string
  状态: string
  可计算: boolean
  结果值: string | number | boolean | null
  正式交付: boolean
  阻塞依赖: string[]
  缺失依赖: string[]
  阻塞原因: string
  成环: boolean
}

export type TransformChecklist = {
  清单编号: string
  版本: string
  样本编号: string
  样本值: Record<string, unknown>
  模板版本: string
  规则版本: Record<string, string>
  规则清单: {
    规则编号: string
    规则版本: string
    规则状态: string
    目标字段: string
    源字段: string
    依赖字段: string
    转换函数: string
  }[]
  字段执行序列: TransformFieldEntry[]
  执行次序: string[]
  可计算字段: Record<string, unknown>
  正式值: Record<string, unknown>
  阻塞依赖: Record<string, string[]>
  阻塞字段: Record<string, string[]>
  分支阻塞依赖: Record<string, string[]>
  重跑范围: string[]
  上一清单版本: string
  生成时间: string
}

export type TransformPreview = {
  ok: boolean
  message: string
  样本编号: string
  模板版本: string
  清单版本: string
  字段执行序列: TransformFieldEntry[]
  执行次序: string[]
  可计算字段: Record<string, unknown>
  正式值: Record<string, unknown>
  阻塞字段: Record<string, string[]>
  分支阻塞依赖: Record<string, string[]>
  重跑范围: string[]
  成环字段: string[]
  checklist: TransformChecklist
}

export type TransformDelivery = {
  交付编号: string
  清单版本: string
  样本编号: string
  目标字段: string
  值: unknown
  规则编号: string
  规则版本: string
  状态: string
}

export type TransformResult = {
  ok: boolean
  message: string
  清单版本: string
  样本编号: string
  模板版本: string
  fields: Record<string, unknown>
  正式值: Record<string, unknown>
  本次交付: TransformDelivery[]
  阻塞依赖: Record<string, string[]>
  重跑范围: string[]
  交付记录: TransformDelivery[]
  checklist: TransformChecklist | null
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  if (!response.ok) {
    const detail = await response.json().catch(() => null)
    throw new Error(detail?.detail ?? `转换请求失败（${response.status}）`)
  }
  return (await response.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    const detail = await response.json().catch(() => null)
    throw new Error(detail?.detail ?? `转换结果读取失败（${response.status}）`)
  }
  return (await response.json()) as T
}

export function previewTransform(
  pipelineId: number,
  payload: Record<string, unknown>,
): Promise<TransformPreview> {
  return postJson(`/api/clean_pipeline/${pipelineId}/preview`, payload)
}

export function runFormalTransform(
  pipelineId: number,
  payload: Record<string, unknown>,
): Promise<TransformResult> {
  return postJson(`/api/clean_pipeline/${pipelineId}/transform`, payload)
}

export function fetchChecklist(
  pipelineId: number,
  version?: string,
): Promise<TransformChecklist> {
  const query = version ? `?version=${encodeURIComponent(version)}` : ''
  return getJson(`/api/clean_pipeline/${pipelineId}/checklist${query}`)
}

export function fetchChecklists(pipelineId: number): Promise<{ items: TransformChecklist[]; total: number }> {
  return getJson(`/api/clean_pipeline/${pipelineId}/checklists`)
}

export function fetchTransformResult(pipelineId: number): Promise<TransformResult> {
  return getJson(`/api/clean_pipeline/${pipelineId}/result`)
}

export type RegisteredSample = {
  样本编号: string
  字段值: Record<string, unknown>
  模板版本: string
  来源模板: string
  来源: string
}

export async function fetchSamples(pipelineId: number): Promise<RegisteredSample[]> {
  const payload = await getJson<{ items: RegisteredSample[]; total: number }>(
    `/api/clean_pipeline/${pipelineId}/samples`,
  )
  return payload.items ?? []
}
