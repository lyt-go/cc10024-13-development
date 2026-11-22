<template>
  <section class="transform-panel" data-testid="transform-preview-panel" :data-pipeline="pipelineCode">
    <header class="panel-head">
      <div>
        <h3>清洗样本转换预演</h3>
        <p class="page-desc">
          用真实采集行预演字段依赖：按执行次序展示可计算值，单独列出阻塞依赖。清单冻结样本身份和规则版本，正式转换只消费清单。
        </p>
      </div>
      <div class="panel-pipeline">
        <label class="filter-item">
          <span>清洗工序</span>
          <select v-model.number="activePipelineId" data-testid="transform-pipeline" @change="switchPipeline">
            <option v-for="option in pipelineList" :key="option.id" :value="option.id">
              {{ option.code }}
            </option>
          </select>
        </label>
      </div>
    </header>

    <form class="sample-form" @submit.prevent="preview">
      <label class="filter-item">
        <span>已登记样本</span>
        <select v-model="registeredSampleId" data-testid="transform-registered-sample" @change="applyRegisteredSample">
          <option value="">手工输入</option>
          <option v-for="item in registeredSamples" :key="item.样本编号" :value="item.样本编号">
            {{ item.样本编号 }}（{{ item.来源 || 'data_extract' }}）
          </option>
        </select>
      </label>
      <label class="filter-item">
        <span>样本编号</span>
        <input v-model="sampleId" data-testid="transform-sample-id" placeholder="SAMPLE-PRICE-7" />
      </label>
      <label class="filter-item">
        <span>模板版本</span>
        <input v-model="templateVersion" data-testid="transform-template-version" placeholder="TPL-v3" />
      </label>
      <label class="filter-item sample-values">
        <span>实际样本值（JSON）</span>
        <textarea
          v-model="sampleValues"
          data-testid="transform-sample-values"
          rows="2"
          placeholder='{"价格": "7", "商品分类原值": "A"}'
        />
      </label>
      <div class="sample-actions">
        <button class="btn" data-testid="transform-preview" type="submit" :disabled="busy">
          {{ busy ? '预演中…' : '预演转换' }}
        </button>
        <button class="btn primary" data-testid="transform-submit" type="button" @click="submitChecklist">
          提交清单并正式转换
        </button>
        <button class="btn ghost" data-testid="transform-load-result" type="button" @click="loadResult">
          读取正式结果
        </button>
      </div>
    </form>

    <p v-if="errorMessage" class="error-text" data-testid="transform-error">{{ errorMessage }}</p>

    <div v-if="summary" class="transform-summary">
      <article class="stat-card">
        <span class="stat-label">样本身份</span>
        <strong class="stat-value small">{{ summary.sampleId }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">模板版本</span>
        <strong class="stat-value small">{{ summary.templateVersion || '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">清单版本</span>
        <strong class="stat-value small">{{ summary.checklistVersion }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">阻塞字段</span>
        <strong class="stat-value small">{{ summary.blocked.length }}</strong>
      </article>
    </div>

    <table v-if="fields.length" class="data-table" data-testid="transform-field-table">
      <thead>
        <tr>
          <th>执行次序</th>
          <th>目标字段</th>
          <th>规则编号</th>
          <th>规则版本</th>
          <th>依赖字段</th>
          <th>状态</th>
          <th>可计算值</th>
          <th>阻塞依赖</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="field in fields"
          :key="`${field.规则编号}-${field.执行序号}`"
          :data-testid="`transform-field-${field.目标字段}-${field.规则编号}`"
          :data-status="field.状态"
          :class="{ 'row-blocked': field.状态 === '阻塞' }"
        >
          <td>{{ field.执行序号 }}</td>
          <td>{{ field.目标字段 }}</td>
          <td>{{ field.规则编号 }}</td>
          <td>{{ field.规则版本 }}</td>
          <td>{{ field.依赖字段 || '—' }}</td>
          <td>
            <span :class="['badge', field.状态 === '可计算' ? 'badge-ok' : field.状态 === '重复目标' ? 'badge-warn' : 'badge-blocked']">
              {{ field.状态 }}
            </span>
          </td>
          <td data-testid="transform-value">{{ field.结果值 ?? '—' }}</td>
          <td>{{ field.阻塞依赖.length ? field.阻塞依赖.join('、') : '—' }}</td>
        </tr>
      </tbody>
    </table>

    <div v-if="blockedRows.length" class="blocked-box" data-testid="transform-blocked">
      <h4>阻塞依赖</h4>
      <p v-for="item in blockedRows" :key="item.field">
        <strong>{{ item.field }}</strong>：{{ item.dependencies.join('、') }}
      </p>
    </div>

    <div v-if="checklist" class="checklist-box" data-testid="transform-checklist">
      <header class="checklist-head">
        <h4>字段转换执行清单 {{ checklist.版本 }}</h4>
        <label class="filter-item">
          <span>历史清单版本</span>
          <select v-model="selectedVersion" data-testid="transform-checklist-version" @change="loadChecklist">
            <option value="">当前清单</option>
            <option v-for="item in checklistVersions" :key="item.版本" :value="item.版本">
              {{ item.版本 }}
            </option>
          </select>
        </label>
      </header>
      <dl class="checklist-meta">
        <div><dt>清单编号</dt><dd>{{ checklist.清单编号 }}</dd></div>
        <div><dt>样本身份</dt><dd>{{ checklist.样本编号 }}</dd></div>
        <div><dt>模板版本</dt><dd>{{ checklist.模板版本 || '—' }}</dd></div>
        <div><dt>重跑范围</dt><dd>{{ checklist.重跑范围.join('、') || '—' }}</dd></div>
        <div><dt>正式值</dt><dd>{{ formatValues(checklist.正式值) }}</dd></div>
        <div><dt>阻塞字段</dt><dd>{{ formatBlocked(checklist.阻塞字段 ?? checklist.阻塞依赖) }}</dd></div>
        <div><dt>分支阻塞依赖</dt><dd>{{ formatBlocked(checklist.分支阻塞依赖 ?? {}) }}</dd></div>
      </dl>
      <table class="data-table">
        <thead>
          <tr><th>目标字段</th><th>规则编号</th><th>规则版本</th><th>实际版本指纹</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="rule in checklist.规则清单" :key="`${rule.规则编号}-${rule.规则版本}`">
            <td>{{ rule.目标字段 }}</td>
            <td>{{ rule.规则编号 }}</td>
            <td>{{ rule.规则版本 }}</td>
            <td>{{ (checklist.规则版本 || {})[rule.规则编号] ?? '—' }}</td>
            <td>{{ rule.规则状态 || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="deliveries.length" class="deliveries-box" data-testid="transform-deliveries">
      <h4>正式交付记录</h4>
      <p class="page-desc">同一目标只保留一个有效值，历史交付仍可回查。</p>
      <table class="data-table">
        <thead>
          <tr><th>目标字段</th><th>值</th><th>规则</th><th>清单版本</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in deliveries" :key="item.交付编号">
            <td>{{ item.目标字段 }}</td>
            <td>{{ item.值 ?? '—' }}</td>
            <td>{{ item.规则编号 }}（{{ item.规则版本 }}）</td>
            <td>{{ item.清单版本 }}</td>
            <td>{{ item.状态 }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  fetchChecklists,
  fetchChecklist,
  fetchSamples,
  fetchTransformResult,
  previewTransform,
  runFormalTransform,
} from '@/api/transform-service'
import type {
  RegisteredSample,
  TransformChecklist,
  TransformDelivery,
  TransformFieldEntry,
  TransformPreview,
  TransformResult,
} from '@/api/transform-service'

type PipelineOption = { id: number; code: string }

const props = defineProps<{ pipelines: PipelineOption[]; currentPipelineId?: number }>()

const pipelineList = computed(() =>
  props.pipelines.length
    ? props.pipelines
    : props.currentPipelineId
      ? [{ id: props.currentPipelineId, code: `工序#${props.currentPipelineId}` }]
      : [],
)

const activePipelineId = ref<number>(
  props.currentPipelineId ?? props.pipelines[0]?.id ?? 0,
)
const sampleId = ref('SAMPLE-PRICE-7')
const templateVersion = ref('TPL-v3')
const sampleValues = ref('{"价格": "7", "商品分类原值": "A"}')
const registeredSamples = ref<RegisteredSample[]>([])
const registeredSampleId = ref('')
const fields = ref<TransformFieldEntry[]>([])
const checklist = ref<TransformChecklist | null>(null)
const checklistVersions = ref<TransformChecklist[]>([])
const selectedVersion = ref('')
const deliveries = ref<TransformDelivery[]>([])
const summary = ref<{
  sampleId: string
  templateVersion: string
  checklistVersion: string
  blocked: string[]
} | null>(null)
const errorMessage = ref('')
const busy = ref(false)

const pipelineCode = computed(
  () => pipelineList.value.find((item) => item.id === activePipelineId.value)?.code ?? '',
)

const blockedRows = computed(() => {
  const source = checklist.value?.阻塞字段 ?? checklist.value?.阻塞依赖 ?? {}
  return Object.entries(source)
    .filter(([, dependencies]) => dependencies.length > 0)
    .map(([field, dependencies]) => ({ field, dependencies }))
})

function parseSample(): Record<string, unknown> {
  const text = sampleValues.value.trim()
  if (!text) return {}
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    throw new Error('样本值必须是合法 JSON，例如 {"价格": "7", "商品分类原值": "A"}')
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('样本值必须是字段到值的对象')
  }
  return parsed as Record<string, unknown>
}

function samplePayload() {
  const values = parseSample()
  const identity = sampleId.value.trim()
  if (!Object.keys(values).length && identity) {
    // 已登记的样本（例如 data_extract 交付的真实采集行）按样本编号引用
    return { sample_id: identity }
  }
  return {
    sample: {
      样本编号: identity,
      字段值: values,
      模板版本: templateVersion.value.trim(),
    },
  }
}

function applyRegisteredSample() {
  const selected = registeredSamples.value.find((item) => item.样本编号 === registeredSampleId.value)
  if (!selected) return
  sampleId.value = selected.样本编号
  templateVersion.value = selected.模板版本 ?? ''
  sampleValues.value = JSON.stringify(selected.字段值 ?? {}, null, 0)
}

async function loadSamples() {
  if (!activePipelineId.value) return
  try {
    registeredSamples.value = await fetchSamples(activePipelineId.value)
  } catch {
    registeredSamples.value = []
  }
}

function formatValues(values: Record<string, unknown>): string {
  const entries = Object.entries(values ?? {})
  if (!entries.length) return '—'
  return entries.map(([key, value]) => `${key}=${String(value)}`).join('，')
}

function formatBlocked(blocked: Record<string, string[]> | undefined): string {
  const entries = Object.entries(blocked ?? {})
  if (!entries.length) return '—'
  return entries.map(([key, value]) => `${key}←${value.join('、')}`).join('；')
}

function applyPreview(payload: TransformPreview) {
  fields.value = [...(payload.字段执行序列 ?? [])].sort((a, b) => a.执行序号 - b.执行序号)
  checklist.value = payload.checklist
  summary.value = {
    sampleId: payload.样本编号,
    templateVersion: payload.模板版本,
    checklistVersion: payload.清单版本,
    blocked: Object.keys(payload.阻塞字段 ?? {}),
  }
}

function applyResult(payload: TransformResult) {
  checklist.value = payload.checklist
  deliveries.value = payload.交付记录 ?? []
  summary.value = {
    sampleId: payload.样本编号,
    templateVersion: payload.模板版本,
    checklistVersion: payload.清单版本,
    blocked: Object.keys(payload.阻塞依赖 ?? {}),
  }
  if (payload.checklist?.字段执行序列) {
    fields.value = [...payload.checklist.字段执行序列].sort((a, b) => a.执行序号 - b.执行序号)
  }
}

async function preview() {
  if (!activePipelineId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    applyPreview(await previewTransform(activePipelineId.value, samplePayload()))
    await loadVersions()
    await loadSamples()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '转换预演失败'
  } finally {
    busy.value = false
  }
}

async function submitChecklist() {
  if (!activePipelineId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const payload = await runFormalTransform(activePipelineId.value, samplePayload())
    applyResult(payload)
    await loadVersions()
    await loadSamples()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '正式转换失败'
  } finally {
    busy.value = false
  }
}

async function loadResult() {
  if (!activePipelineId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    applyResult(await fetchTransformResult(activePipelineId.value))
    await loadVersions()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '正式结果读取失败'
  } finally {
    busy.value = false
  }
}

async function loadVersions() {
  if (!activePipelineId.value) return
  try {
    const payload = await fetchChecklists(activePipelineId.value)
    checklistVersions.value = payload.items
  } catch {
    checklistVersions.value = []
  }
}

async function loadChecklist() {
  if (!activePipelineId.value) return
  errorMessage.value = ''
  try {
    checklist.value = await fetchChecklist(activePipelineId.value, selectedVersion.value || undefined)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '清单读取失败'
  }
}

function switchPipeline() {
  fields.value = []
  checklist.value = null
  checklistVersions.value = []
  deliveries.value = []
  summary.value = null
  errorMessage.value = ''
  registeredSampleId.value = ''
  registeredSamples.value = []
  void loadVersions()
  void loadSamples()
}

onMounted(() => {
  void loadVersions()
  void loadSamples()
})
</script>
