const validName = name => typeof name === "string" &&
  /[A-Za-zÀ-ÿ]/.test(name) &&
  name.trim().length > 1 &&
  name.length <= 120 &&
  !/@(?:lid|s\.whatsapp\.net|g\.us)/i.test(name) &&
  !/^\s*\d[\d\s()+.-]*\s*$/.test(name);

export async function peopleDirectory(pool, instance) {
  const result = await pool.query(
    "SELECT jid,display_name FROM cg_people WHERE instance=$1 AND display_name IS NOT NULL",
    [instance]
  );
  const names = new Map();
  for (const row of result.rows) {
    if (!validName(row.display_name)) continue;
    const value = row.display_name.trim();
    const id = String(row.jid).split("@")[0];
    if (/^\d{10,18}$/.test(id)) names.set(id, value);
    names.set(row.jid, value);
  }
  return names;
}

export function readablePerson(source, names) {
  const raw = String(source || "").trim();
  if (validName(raw)) return raw;
  const id = raw.split("@")[0];
  if (names.has(raw)) return names.get(raw);
  if (names.has(id)) return names.get(id);
  if (/^\d{10,18}$/.test(id)) return "Participante não identificado (final " + id.slice(-4) + ")";
  return raw || "Participante não identificado";
}

export function resolveIdentifiers(text, names) {
  let out = String(text || "");
  // Replace known Evolution LIDs, including IDs without a leading @ in existing summaries.
  for (const [id, name] of names) {
    if (!/^\d{12,18}$/.test(id)) continue;
    const re = new RegExp("(^|[^0-9])(@?)" + id + "(?![0-9])", "g");
    out = out.replace(re, (_all, before, at) => before + at + name);
  }
  // Unresolved @LIDs get a readable, explicit placeholder, not an invented name.
  out = out.replace(/@(\d{12,18})(?!\d)/g, (_all, id) =>
    "@Participante não identificado (final " + id.slice(-4) + ")");
  // Numeric LIDs named as owners in old saved summaries.
  out = out.replace(/\b(Responsáve(?:l|is)\s*:\s*)(\d{12,18})(?!\d)/gi,
    (_all, label, id) => label + "Participante não identificado (final " + id.slice(-4) + ")");
  return out;
}
