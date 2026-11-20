<template>
  <section class="page" data-module="window_agg">
    <header class="page-head">
      <div>
        <h2>窗口聚合管理</h2>
        <p class="page-desc">维护流式窗口聚合的定义、迟到数据处理和状态过期策略。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记时间窗口</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出窗口聚合清单</button>
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
          <td :colspan="columns.length + 1" class="empty-state">暂无窗口聚合数据，可先登记时间窗口</td>
        </tr>
      </tbody>
    </table>

    <section class="bucket-panel" data-testid="bucket-panel">
      <header class="panel-head">
        <div>
          <h3>分窗执行与订单归桶</h3>
          <p class="page-desc">
            窗口边界取自窗口配置；滚动窗口按窗口长度对齐，滑动窗口按滑动步长枚举，
            同一订单可进入多个重叠桶，同一桶内重复补投只计一次。
          </p>
        </div>
        <div class="page-actions">
          <select v-model="bucketQuery.window_id" data-testid="bucket-window" @change="reloadBuckets">
            <option value="">全部窗口</option>
            <option v-for="cfg in windowConfigs" :key="cfg.窗口编号" :value="cfg.窗口编号">
              {{ cfg.窗口编号 }}
            </option>
          </select>
          <button class="btn primary" data-testid="run-buckets" type="button" :disabled="runningBuckets" @click="runBuckets">
            {{ runningBuckets ? '归桶中…' : '执行分窗' }}
          </button>
          <button class="btn" data-testid="refresh-buckets" type="button" @click="reloadBuckets">刷新分桶</button>
        </div>
      </header>

      <form class="filter-bar" @submit.prevent="reloadBuckets">
        <label class="filter-item">
          <span>事件时段起</span>
          <input v-model="bucketQuery.start" data-testid="bucket-start" placeholder="2026-11-16T08:05:00Z" />
        </label>
        <label class="filter-item">
          <span>事件时段止</span>
          <input v-model="bucketQuery.end" data-testid="bucket-end" placeholder="2026-11-16T08:10:00Z" />
        </label>
        <label class="filter-item">
          <span>订单编号</span>
          <input v-model="bucketQuery.order_id" data-testid="bucket-order" placeholder="按订单定位桶" />
        </label>
        <button class="btn" data-testid="bucket-query" type="submit">按桶查询</button>
      </form>

      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">分桶数</span>
          <strong class="stat-value" data-testid="bucket-count">{{ bucketRows.length }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">分桶金额合计</span>
          <strong class="stat-value" data-testid="bucket-gmv">{{ bucketTotalGmv }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">窗外记录</span>
          <strong class="stat-value" data-testid="outside-count">{{ outsideRows.length }}</strong>
        </article>
      </div>

      <table class="data-table" data-testid="bucket-table">
        <thead>
          <tr>
            <th>桶编号</th>
            <th>窗口时段</th>
            <th>金额</th>
            <th>笔数</th>
            <th>独立用户</th>
            <th>封账</th>
            <th>聚合批次</th>
            <th>记录范围</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="bucket in bucketRows" :key="String(bucket.bucket_id)" :data-testid="`bucket-${bucket.bucket_id}`">
            <td>{{ bucket.bucket_id }}</td>
            <td>{{ bucket.窗口开始 }} ~ {{ bucket.窗口结束 }}</td>
            <td>{{ bucket.金额 ?? bucket.gmv }}</td>
            <td>{{ bucket.记录数 }}</td>
            <td>{{ bucket.独立用户 }}</td>
            <td>{{ bucket.封账 ? '已封账' : '未封账' }}</td>
            <td>{{ bucket.batch_id }} / v{{ bucket.版本 }}</td>
            <td>{{ (bucket.记录范围 || []).join('、') || '—' }}</td>
          </tr>
          <tr v-if="!bucketRows.length">
            <td colspan="8" class="empty-state">暂无分桶数据，先执行一次分窗</td>
          </tr>
        </tbody>
      </table>

      <table v-if="outsideRows.length" class="data-table" data-testid="outside-table">
        <thead>
          <tr><th>窗外订单</th><th>事件时间</th><th>所属窗口</th><th>原本归属桶</th><th>原因</th></tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in outsideRows" :key="`${row.record_id}-${row.window_id}-${index}`">
            <td>{{ row.订单编号 || row.record_id }}</td>
            <td>{{ row.event_time }}</td>
            <td>{{ row.窗口编号 }}</td>
            <td>{{ row.bucket_id || '—' }}</td>
            <td>{{ row.原因 }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记时间窗口</h3>
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
      <span>共 {{ total }} 条窗口聚合记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/window_agg'
const columns = ["窗口编号", "聚合指标", "窗口类型", "窗口长度", "滑动步长", "迟到阈值", "发射频率", "状态过期"]
const actions = ["启效窗口", "暂停窗口", "废弃窗口"]
const statuses = ["草稿", "生效中", "已暂停", "已废弃"]
const metricNames = ["窗口总数", "生效中窗口", "已暂停窗口"]

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

type BucketRow = Record<string, any>
const BUCKET_ENDPOINT = '/api/window_runtime'
const bucketRows = ref<BucketRow[]>([])
const outsideRows = ref<BucketRow[]>([])
const windowConfigs = ref<BucketRow[]>([])
const runningBuckets = ref(false)
const bucketQuery = ref({ window_id: '', start: '', end: '', order_id: '' })
const bucketTotalGmv = computed(() =>
  bucketRows.value.reduce((sum, bucket) => sum + Number(bucket.金额 ?? bucket.gmv ?? 0), 0).toFixed(2),
)

async function runBuckets() {
  runningBuckets.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${BUCKET_ENDPOINT}/run`, {
      method: 'POST',
      body: JSON.stringify({ window_id: bucketQuery.value.window_id || null }),
    })
    if (!response.ok) throw new Error('分窗执行失败，请稍后重试')
    const payload = await response.json()
    windowConfigs.value = payload.result?.窗口配置 ?? []
    await reloadBuckets()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分窗执行失败'
  } finally {
    runningBuckets.value = false
  }
}

async function reloadBuckets() {
  errorMessage.value = ''
  const query = new URLSearchParams()
  if (bucketQuery.value.window_id) query.set('window_id', bucketQuery.value.window_id)
  if (bucketQuery.value.start) query.set('start', bucketQuery.value.start)
  if (bucketQuery.value.end) query.set('end', bucketQuery.value.end)
  if (bucketQuery.value.order_id) query.set('order_id', bucketQuery.value.order_id)
  try {
    const [bucketResponse, outsideResponse, aggregationResponse] = await Promise.all([
      request(`${BUCKET_ENDPOINT}/buckets?${query.toString()}`),
      request(`${BUCKET_ENDPOINT}/outside?${query.toString()}`),
      request(`${BUCKET_ENDPOINT}/aggregation${bucketQuery.value.window_id ? `?window_id=${bucketQuery.value.window_id}` : ''}`),
    ])
    if (!bucketResponse.ok) throw new Error('分桶结果读取失败')
    bucketRows.value = (await bucketResponse.json()).items ?? []
    outsideRows.value = outsideResponse.ok ? (await outsideResponse.json()).items ?? [] : []
    if (aggregationResponse.ok) {
      windowConfigs.value = (await aggregationResponse.json()).result?.窗口配置 ?? []
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分桶结果读取失败'
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
    link.download = 'window_agg.csv'
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
      throw new Error('窗口聚合动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '窗口聚合操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('时间窗口列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '窗口聚合列表读取失败'
  }
}

onMounted(async () => {
  await Promise.all([reload(), reloadBuckets()])
})
</script>
