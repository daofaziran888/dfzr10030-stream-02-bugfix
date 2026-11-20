<template>
  <section class="page" data-module="metric_calc">
    <header class="page-head">
      <div>
        <h2>指标计算管理</h2>
        <p class="page-desc">维护实时指标的计算公式、维度组合和缓存策略，支撑实时看板和告警。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记实时指标</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出指标计算清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无指标计算数据，可先登记实时指标</td>
        </tr>
      </tbody>
    </table>

    <section class="bucket-panel" data-testid="metric-runtime-panel">
      <header class="panel-head">
        <div>
          <h3>指标运行时 · 分窗口径</h3>
          <p class="page-desc">指标计算消费分窗指标聚合结果，每个桶只引用一个批次版本；下面的批次清单就是本次指标实际引用的来源。</p>
        </div>
        <div class="page-actions">
          <select v-model="metricQuery.window_id" data-testid="metric-window">
            <option value="WIN-TUMBLE-60s">WIN-TUMBLE-60s</option>
            <option value="WIN-SLIDE-30s">WIN-SLIDE-30s</option>
            <option value="WIN-SESSION-5m">WIN-SESSION-5m</option>
          </select>
          <button class="btn" data-testid="run-metric" type="button" :disabled="runningMetric" @click="runMetric">
            {{ runningMetric ? '计算中…' : '按窗口重算' }}
          </button>
          <button class="btn primary" data-testid="confirm-bucket" type="button" @click="confirmBucket">确认当前批次</button>
        </div>
      </header>

      <div v-if="metricResult" class="metric-detail" data-testid="metric-detail">
        <dl>
          <dt>窗口编号</dt><dd>{{ metricResult.窗口编号 }}</dd>
          <dt>桶数 / 记录数</dt><dd>{{ metricResult.桶数 }} / {{ metricResult.记录数 }}</dd>
          <dt>金额(去重)</dt><dd>{{ metricResult.gmv }}</dd>
          <dt>独立用户</dt><dd>{{ metricResult.独立用户 }}</dd>
          <dt>最近发射</dt><dd class="batch-ref">{{ metricResult.最近发射?.emission_id || '尚未发射' }}</dd>
        </dl>
        <table class="data-table" data-testid="batch-ref-table">
          <thead>
            <tr><th>桶编号</th><th>聚合批次</th><th>版本</th><th>笔数</th></tr>
          </thead>
          <tbody>
            <tr v-for="ref in metricResult.聚合批次引用" :key="String(ref.bucket_id)" :data-testid="`batch-${ref.bucket_id}`">
              <td>{{ ref.bucket_id }}</td>
              <td class="batch-ref">{{ ref.batch_id }}</td>
              <td>{{ ref.版本 }}</td>
              <td>{{ (metricResult.分桶 || []).find((b: any) => b.bucket_id === ref.bucket_id)?.记录数 ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-else class="page-desc">暂无分窗聚合结果，先在窗口聚合页执行一次分窗。</p>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记实时指标</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条指标计算记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/metric_calc'
const columns = ["指标编号", "指标名称", "计算公式", "更新频率", "维度组合", "精度模式", "缓存窗口", "最后计算"]
const actions = ["发布指标", "暂停计算", "标记异常"]
const statuses = ["草稿", "计算中", "可用", "计算异常"]
const metricNames = ["指标总数", "可用指标", "计算异常指标"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

const METRIC_ENDPOINT = '/api/metric_runtime'
const metricQuery = ref({ window_id: 'WIN-TUMBLE-60s' })
const metricResult = ref<Record<string, any> | null>(null)
const runningMetric = ref(false)

async function runMetric() {
  runningMetric.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${METRIC_ENDPOINT}/run`, {
      method: 'POST',
      body: JSON.stringify({ window_id: metricQuery.value.window_id }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.detail || '指标计算失败')
    metricResult.value = payload.result
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '指标计算失败'
  } finally {
    runningMetric.value = false
  }
}

async function loadMetric() {
  errorMessage.value = ''
  try {
    const response = await request(`${METRIC_ENDPOINT}/result?window_id=${metricQuery.value.window_id}`)
    if (!response.ok) {
      metricResult.value = null
      return
    }
    metricResult.value = (await response.json()).result
  } catch {
    metricResult.value = null
  }
}

async function confirmBucket() {
  const first = metricResult.value?.聚合批次引用?.[0]
  if (!first) {
    errorMessage.value = '当前没有可确认的聚合批次'
    return
  }
  errorMessage.value = ''
  try {
    const response = await request('/api/window_runtime/confirm', {
      method: 'POST',
      body: JSON.stringify({
        bucket_id: first.bucket_id,
        batch_id: first.batch_id,
        emission_id: metricResult.value?.最近发射?.emission_id,
      }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '确认失败')
    await loadMetric()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '确认失败'
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'metric_calc.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('指标计算动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '指标计算操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('实时指标列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '指标计算列表读取失败'
  }
}

onMounted(async () => {
  await Promise.all([reload(), loadMetric()])
})
</script>
