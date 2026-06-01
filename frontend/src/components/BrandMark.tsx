type BrandMarkProps = {
  compact?: boolean
}

export function BrandMark({ compact = false }: BrandMarkProps) {
  return (
    <div className="brand-mark" aria-label="NodusDB">
      <span className="brand-mark__glyph" aria-hidden="true">
        N
      </span>
      {!compact ? (
        <span className="brand-mark__text">
          <strong>NodusDB</strong>
          <small>Control plane</small>
        </span>
      ) : null}
    </div>
  )
}
