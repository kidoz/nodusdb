type PermissionExplorerProps = {
  principalPlaceholder?: string
  resourcePlaceholder?: string
}

export function PermissionExplorer({
  principalPlaceholder = "Principal, e.g. 'alice'",
  resourcePlaceholder = "Resource, e.g. 'appdb.billing.users'",
}: PermissionExplorerProps) {
  return (
    <section className="permission-panel" aria-labelledby="permission-title">
      <div className="section-heading">
        <p>Authorization</p>
        <h2 id="permission-title">RBAC permission explorer</h2>
      </div>
      <form className="permission-form">
        <label>
          <span>Principal</span>
          <input type="text" placeholder={principalPlaceholder} />
        </label>
        <label>
          <span>Resource</span>
          <input type="text" placeholder={resourcePlaceholder} />
        </label>
        <button type="button">Check access</button>
      </form>
      <div className="permission-result">
        Enter a principal and resource to evaluate the authorization graph.
      </div>
    </section>
  )
}
