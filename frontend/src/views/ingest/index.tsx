import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/ingest'
const batchColumns = ["批次编号", "业务日期", "表编号", "批次状态", "进度", "原始行数", "去重后行数", "缺口说明"]
const unitColumns = ["分片序号", "原始路径", "行数", "字节大小", "摘要算法", "校验摘要", "状态", "失败原因"]
const availabilityColumns = ["表编号", "表名称", "业务日期", "可用状态", "可用范围", "当前生效版本"]
const versionColumns = ["版本号", "业务日期", "表编号", "分片清单", "分片数", "总行数", "去重后行数", "摘要算法", "校验摘要", "校验时刻", "状态"]
const auditColumns = ["记录编号", "变更类型", "变更前值", "变更后值", "变更原因", "操作时间"]

type Row = Record<string, string | number | boolean | null | undefined>
type Part = { 分片序号: number; 原始路径: string; 行数: number; 字节大小: number; 校验摘要: string }

type BatchDetail = {
  批次编号: string
  业务日期: string
  表编号: string
  批次状态: string
  完成分片数: number
  分片总数: number
  进度: string
  缺口说明: string
  可用状态: string
  可用范围: string
  生效版本: string | null
  迟到分片: boolean
  分片内重复主键数: number
  分片: Row[]
  审计: Row[]
}

const emptyPart = (): Part => ({ 分片序号: 1, 原始路径: '', 行数: 0, 字节大小: 0, 校验摘要: '' })

export default function IngestView() {
  const [batches, setBatches] = useState<Row[]>([])
  const [availability, setAvailability] = useState<Row[]>([])
  const [versions, setVersions] = useState<Row[]>([])
  const [detail, setDetail] = useState<BatchDetail | null>(null)
  const [resume, setResume] = useState<Row | null>(null)
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [verifyRows, setVerifyRows] = useState<Record<string, number>>({})

  const [form, setForm] = useState({
    批次编号: '',
    来源系统: '订单库',
    落地路径: 's3://raw-dw/order/2026-10-12',
    文件格式: 'csv',
    业务日期: '2026-10-12',
    表编号: 'ODS-ORDER',
    声明总行数: '',
  })
  const [parts, setParts] = useState<Part[]>([emptyPart()])
  const [latePart, setLatePart] = useState<Part & { 迟到原因: string }>({ ...emptyPart(), 迟到原因: '' })

  const reload = useCallback(async () => {
    setErrorMessage('')
    try {
      const [batchRes, availRes, versionRes] = await Promise.all([
        request(`${ENDPOINT}/batches?size=100`),
        request(`${ENDPOINT}/availability`),
        request(`${ENDPOINT}/versions`),
      ])
      if (!batchRes.ok || !availRes.ok || !versionRes.ok) {
        throw new Error('入湖编排数据读取失败')
      }
      const batchPayload = await batchRes.json()
      setBatches(batchPayload.items ?? [])
      setAvailability(await availRes.json())
      setVersions(await versionRes.json())
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '入湖编排数据读取失败')
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  const stats = useMemo(() => {
    const landed = batches.filter((row) => row.status === '已落地').length
    const landing = batches.filter((row) => row.status === '落地中').length
    const usable = availability.filter((row) => row.可用状态 === '可用').length
    return [
      { label: '入湖批次', value: batches.length },
      { label: '已发布批次', value: landed },
      { label: '落地中批次', value: landing },
      { label: '可用贴源表', value: usable },
    ]
  }, [batches, availability])

  async function loadDetail(batchNo: string) {
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/batches/${encodeURIComponent(batchNo)}`)
      if (!response.ok) {
        throw new Error('批次明细读取失败')
      }
      const payload = await response.json()
      if (payload.ok === false) {
        throw new Error(payload.message ?? '批次明细读取失败')
      }
      setDetail(payload as BatchDetail)
      setResume(null)
      const rows: Record<string, number> = {}
      for (const unit of payload.分片 ?? []) {
        rows[String(unit.分片序号)] = Number(unit.行数 ?? 0)
      }
      setVerifyRows(rows)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '批次明细读取失败')
    }
  }

  async function postAction(path: string, values: Record<string, unknown>) {
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(path, { method: 'POST', body: JSON.stringify({ values }) })
      const payload = await response.json()
      if (!response.ok || !payload.ok) {
        throw new Error(payload.message ?? '入湖动作未生效')
      }
      setMessage(payload.message ?? '动作已提交')
      if (detail) {
        await loadDetail(detail.批次编号)
      }
      await reload()
      return payload.entry as Row
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '入湖动作失败')
      return null
    }
  }

  async function submitBatch() {
    const payload = {
      ...form,
      声明总行数: form.声明总行数 === '' ? undefined : Number(form.声明总行数),
      分片清单: parts.map((part) => ({ ...part, 行数: Number(part.行数), 字节大小: Number(part.字节大小) })),
    }
    await postAction(`${ENDPOINT}/batches`, payload)
  }

  function updatePart(index: number, key: keyof Part, value: string) {
    setParts((prev) => prev.map((part, i) => (
      i === index ? { ...part, [key]: key === '原始路径' || key === '校验摘要' ? value : Number(value) } : part
    )))
  }

  const canPublish = detail ? detail.完成分片数 === detail.分片总数 && detail.分片总数 > 0 : false

  return (
    <section className="page" data-module="ingest">
      <header className="page-head">
        <div>
          <h2>数仓入湖编排</h2>
          <p className="page-desc">
            原始文件登记与贴源表加载共用一次入湖提交：分片逐个校验，全部齐备才发布贴源表版本，
            任一分片失败时批次停在落地中，贴源表保持不可用。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn" type="button" onClick={() => void reload()}>刷新</button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <h3 className="section-title">登记入湖批次（来源系统 · 文件格式 · 原始路径 · 字节大小 / 分片清单）</h3>
      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void submitBatch() }}>
        {(['批次编号', '来源系统', '落地路径', '文件格式', '业务日期', '表编号', '声明总行数'] as const).map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={String(form[field])}
              placeholder={field}
              onChange={(event) => setForm((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {['分片序号', '原始路径', '行数', '字节大小', '校验摘要'].map((column) => <th key={column}>{column}</th>)}
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {parts.map((part, index) => (
            <tr key={index}>
              <td>
                <input className="cell-input" value={part.分片序号} onChange={(event) => updatePart(index, '分片序号', event.target.value)} />
              </td>
              <td>
                <input className="cell-input" value={part.原始路径} placeholder="原始路径" onChange={(event) => updatePart(index, '原始路径', event.target.value)} />
              </td>
              <td>
                <input className="cell-input" value={part.行数} onChange={(event) => updatePart(index, '行数', event.target.value)} />
              </td>
              <td>
                <input className="cell-input" value={part.字节大小} onChange={(event) => updatePart(index, '字节大小', event.target.value)} />
              </td>
              <td>
                <input className="cell-input" value={part.校验摘要} placeholder="sha256:..." onChange={(event) => updatePart(index, '校验摘要', event.target.value)} />
              </td>
              <td className="row-actions">
                <button className="link" type="button" onClick={() => setParts((prev) => prev.filter((_, i) => i !== index))}>
                  移除
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="page-actions" style={{ marginTop: 8 }}>
        <button className="btn ghost" type="button" onClick={() => setParts((prev) => [...prev, { ...emptyPart(), 分片序号: prev.length + 1 }])}>
          增加分片
        </button>
        <button className="btn primary" type="button" disabled={!form.批次编号} onClick={() => void submitBatch()}>
          登记入湖批次
        </button>
      </div>

      <h3 className="section-title">入湖批次进度</h3>
      <table className="data-table">
        <thead>
          <tr>
            {batchColumns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {batches.map((row) => (
            <tr key={String(row.id)}>
              {batchColumns.map((column) => (
                <td key={column} className={column === '缺口说明' ? 'gap-cell' : undefined}>{row[column] || '—'}</td>
              ))}
              <td className="row-actions">
                <button className="link" type="button" onClick={() => void loadDetail(String(row.批次编号))}>详情</button>
              </td>
            </tr>
          ))}
          {batches.length === 0 && (
            <tr><td className="empty-state" colSpan={batchColumns.length + 1}>暂无入湖批次，先登记源端清单</td></tr>
          )}
        </tbody>
      </table>

      {detail && (
        <section className="detail">
          <h3 className="section-title">批次 {detail.批次编号} 的分片与审计（同一次入湖提交）</h3>
          <p className="page-desc">
            进度 {detail.进度}，可用状态 {detail.可用状态}；{detail.缺口说明 || '全部分片已通过校验'}
          </p>
          <table className="data-table">
            <thead>
              <tr>
                {unitColumns.map((column) => <th key={column}>{column}</th>)}
                <th>复核行数 / 动作</th>
              </tr>
            </thead>
            <tbody>
              {detail.分片.map((unit) => (
                <tr key={String(unit.id)}>
                  {unitColumns.map((column) => (
                    <td key={column}>
                      {column === '校验摘要' ? <span className="mono">{String(unit[column] ?? '—')}</span> : (unit[column] || '—')}
                    </td>
                  ))}
                  <td className="row-actions">
                    {detail.批次状态 !== '已落地' ? (
                      <>
                        <input
                          className="cell-input narrow"
                          value={verifyRows[String(unit.分片序号)] ?? ''}
                          onChange={(event) => setVerifyRows((prev) => ({ ...prev, [String(unit.分片序号)]: Number(event.target.value) }))}
                        />
                        <button
                          className="link"
                          type="button"
                          onClick={() => void postAction(`${ENDPOINT}/batches/${detail.批次编号}/units/${unit.分片序号}/verify`, { 行数: verifyRows[String(unit.分片序号)] })}
                        >
                          {unit.状态 === '已完成' ? '已复用' : '复核'}
                        </button>
                      </>
                    ) : <span className="page-desc">版本已发布，分片只读</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="page-actions" style={{ marginTop: 8 }}>
            <button
              className="btn primary"
              type="button"
              disabled={!canPublish || detail.批次状态 === '已落地'}
              onClick={() => void postAction(`${ENDPOINT}/batches/${detail.批次编号}/publish`, {})}
            >
              发布贴源表版本
            </button>
            <button
              className="btn"
              type="button"
              onClick={async () => {
                const response = await request(`${ENDPOINT}/batches/${detail.批次编号}/resume`)
                setResume(await response.json())
              }}
            >
              恢复计划
            </button>
            {detail.批次状态 !== '已落地' && (
              <button
                className="btn"
                type="button"
                onClick={() => void postAction(`${ENDPOINT}/batches/${detail.批次编号}/units`, {
                  ...latePart,
                  行数: Number(latePart.行数),
                  字节大小: Number(latePart.字节大小),
                  迟到原因: latePart.迟到原因 || '源端补传延迟',
                })}
              >
                补登记迟到分片
              </button>
            )}
            {detail.批次状态 !== '已落地' && (
              <button
                className="btn"
                type="button"
                onClick={() => void postAction(`${ENDPOINT}/batches/${detail.批次编号}/recompute`, {
                  分片序号: Number(latePart.分片序号),
                  内容: latePart.校验摘要,
                })}
              >
                按读取内容重算摘要
              </button>
            )}
          </div>

          {detail.批次状态 !== '已落地' && (
            <form className="filter-bar" style={{ marginTop: 8 }} onSubmit={(event) => { event.preventDefault() }}>
              {(['分片序号', '原始路径', '行数', '字节大小', '校验摘要'] as const).map((field) => (
                <label key={field} className="filter-item">
                  <span>{field}</span>
                  <input
                    value={String(latePart[field])}
                    placeholder={field}
                    onChange={(event) => setLatePart((prev) => ({
                      ...prev,
                      [field]: field === '分片序号' || field === '行数' || field === '字节大小'
                        ? Number(event.target.value)
                        : event.target.value,
                    }))}
                  />
                </label>
              ))}
            </form>
          )}

          {resume && (
            <div className="resume-box">
              <strong>{String(resume.说明 ?? '')}</strong>
              <p className="page-desc">
                需复核：{Array.isArray(resume.需复核) && resume.需复核.length > 0
                  ? resume.需复核.map((item: Row) => `分片 ${item.分片序号}（${item.状态}）`).join('、')
                  : '无'}
                ；可复用：{Array.isArray(resume.可复用) ? resume.可复用.map((item: Row) => `分片 ${item.分片序号}`).join('、') || '无' : '无'}
              </p>
            </div>
          )}

          <h3 className="section-title">审计轨迹（每次尝试的失败原因与最终通过摘要）</h3>
          <table className="data-table">
            <thead>
              <tr>{auditColumns.map((column) => <th key={column}>{column}</th>)}</tr>
            </thead>
            <tbody>
              {detail.审计.map((record) => (
                <tr key={String(record.id)}>
                  {auditColumns.map((column) => (
                    <td key={column}>{column === '变更前值' || column === '变更后值' ? <span className="mono">{String(record[column] ?? '—')}</span> : (record[column] ?? '—')}</td>
                  ))}
                </tr>
              ))}
              {detail.审计.length === 0 && (
                <tr><td className="empty-state" colSpan={auditColumns.length}>暂无审计记录</td></tr>
              )}
            </tbody>
          </table>
        </section>
      )}

      <h3 className="section-title">贴源表可用范围</h3>
      <table className="data-table">
        <thead>
          <tr>{availabilityColumns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {availability.map((row) => (
            <tr key={String(row.表编号)}>
              {availabilityColumns.map((column) => (
                <td key={column}>
                  {column === '可用状态'
                    ? <span className={row.可用状态 === '可用' ? 'tag ok' : 'tag warn'}>{String(row.可用状态)}</span>
                    : (row[column] || '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>

      <h3 className="section-title">贴源表版本（已发布版本不重写，冲突以先完整落库版本为准）</h3>
      <table className="data-table">
        <thead>
          <tr>{versionColumns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {versions.map((row) => (
            <tr key={String(row.id)}>
              {versionColumns.map((column) => (
                <td key={column} className={column === '校验摘要' ? 'mono' : undefined}>{row[column] ?? '—'}</td>
              ))}
            </tr>
          ))}
          {versions.length === 0 && (
            <tr><td className="empty-state" colSpan={versionColumns.length}>暂无已发布版本</td></tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>{message}</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
