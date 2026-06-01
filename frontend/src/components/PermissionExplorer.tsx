import { useState } from "react"

type PermissionExplorerProps = {
  principalPlaceholder?: string
  resourcePlaceholder?: string
  apiBase?: string
}

type Explanation = {
  is_allowed: boolean
  steps: string[]
}

const ACTIONS = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE_TABLE"]

export function PermissionExplorer({
  principalPlaceholder = "Principal id (uuid)",
  resourcePlaceholder = "Table, e.g. 'users'",
  apiBase = "http://127.0.0.1:8088",
}: PermissionExplorerProps) {
  const [principal, setPrincipal] = useState("")
  const [resource, setResource] = useState("")
  const [action, setAction] = useState(ACTIONS[0])
  const [result, setResult] = useState<Explanation | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function checkAccess(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      const params = new URLSearchParams({ principal, action })
      if (resource.trim()) params.set("table", resource.trim())
      const res = await fetch(`${apiBase}/api/v1/authz/explain?${params.toString()}`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      setResult((await res.json()) as Explanation)
    } catch (err) {
      setError(err instanceof Error ? err.message : "request failed")
    } finally {
      setLoading(false)
    }
  }

  return (
    <section className="permission-panel" aria-labelledby="permission-title">
      <div className="section-heading">
        <p>Authorization</p>
        <h2 id="permission-title">RBAC permission explorer</h2>
      </div>
      <form className="permission-form" onSubmit={checkAccess}>
        <label>
          <span>Principal</span>
          <input
            type="text"
            placeholder={principalPlaceholder}
            value={principal}
            onChange={(e) => setPrincipal(e.target.value)}
          />
        </label>
        <label>
          <span>Action</span>
          <select value={action} onChange={(e) => setAction(e.target.value)}>
            {ACTIONS.map((a) => (
              <option key={a} value={a}>
                {a}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span>Resource</span>
          <input
            type="text"
            placeholder={resourcePlaceholder}
            value={resource}
            onChange={(e) => setResource(e.target.value)}
          />
        </label>
        <button type="submit" disabled={loading}>
          {loading ? "Checking…" : "Check access"}
        </button>
      </form>
      <div className="permission-result">
        {error && <span className="permission-error">Error: {error}</span>}
        {!error &&
          !result &&
          "Enter a principal and resource to evaluate the authorization graph."}
        {result && (
          <div>
            <strong>{result.is_allowed ? "ALLOW" : "DENY"}</strong>
            <ol>
              {result.steps.map((step, i) => (
                <li key={i}>{step}</li>
              ))}
            </ol>
          </div>
        )}
      </div>
    </section>
  )
}
