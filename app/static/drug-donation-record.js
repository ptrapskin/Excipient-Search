// Drug Repository Donation Record — client-side only.
// GS1 DataMatrix parsing + GTIN->NDC candidate generation + openFDA lookup +
// camera/keyboard-wedge scanning live in the shared gs1-scanner.js (loaded
// before this file) — see that file for those details.
//
// HIPAA-risk-minimizing design for the Donor Information fields (Name,
// Street Address, City, State, Zip): these identify a specific person tied
// to a health-related transaction, so unlike every other field on this page
// (recipient facility name, drug/item data, dates) they are:
//   1. Never persisted anywhere (no localStorage/sessionStorage/cookies) —
//      see the persistFields list below, which deliberately excludes them.
//   2. Auto-cleared after 5 minutes of no donor-field input (see
//      DONOR_IDLE_CLEAR_MS) and immediately cleared from memory the moment
//      a record is generated — the printed/saved output becomes the durable
//      record from that point forward, per SVdP's own PHI handling process.
//   3. Clearable on demand via a standalone "Clear Donor Info Now" button,
//      independent of the general "Reset Form" action.
//   4. Marked autocomplete="off" + spellcheck="false" so the browser never
//      offers to save/auto-suggest them or send them to a cloud spellcheck
//      service.
//   5. Never placed in a URL, query string, or document.title — this page
//      has no client-side routing/query-param state at all.
//   6. Never passed to gtag()/analytics or any fetch() call. The only
//      network request this file makes is the openFDA drug lookup (drug
//      data only, via the shared lookupDrugByGtin()) — GA4's default
//      enhanced-measurement form events (loaded site-wide in base.html)
//      capture only that a form was interacted with, not field values, and
//      nothing in this file customizes that behavior.
//   7. Never submitted anywhere automatically — this tool is print/save
//      only, same as the destruction record tool.

const state = {
  items: [], // {name, strength, ndc, lot, expiration, quantity, unit}
};

const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) ||
  (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);

// ---------- Persistence for recipient (facility) default only ----------
// Donor fields are patient-identifying and are NEVER persisted to storage.
const persistFields = ['recipientName', 'recipientNameOther'];
persistFields.forEach(id => {
  const el = document.getElementById(id);
  const saved = localStorage.getItem('donation_' + id);
  if (saved) el.value = saved;
  el.addEventListener('change', () => localStorage.setItem('donation_' + id, el.value));
});

// ---------- Recipient "Other" toggle ----------
function getRecipientName() {
  const sel = document.getElementById('recipientName');
  return sel.value === '__other__' ? document.getElementById('recipientNameOther').value : sel.value;
}

function updateRecipientOtherVisibility() {
  const isOther = document.getElementById('recipientName').value === '__other__';
  document.getElementById('recipientNameOtherField').style.display = isOther ? '' : 'none';
}

document.getElementById('recipientName').addEventListener('change', updateRecipientOtherVisibility);
updateRecipientOtherVisibility();

(function setDefaultDates() {
  const today = new Date().toISOString().slice(0, 10);
  document.getElementById('dateDonated').value = today;
  document.getElementById('dateSigned').value = today;
})();

if (isIOS) {
  const hint = document.getElementById('iosPrintHint');
  if (hint) hint.style.display = 'block';
}

// ---------- Donor info idle auto-clear (patient-identifying data hygiene) ----------
let DONOR_IDLE_CLEAR_MS = 5 * 60 * 1000; // 5 minutes — adjust here if needed
const donorFieldIds = ['donorName', 'donorAddress', 'donorCity', 'donorState', 'donorZip'];
let donorIdleTimer = null;

function clearDonorFields(showNotice) {
  donorFieldIds.forEach(id => {
    document.getElementById(id).value = (id === 'donorState' ? 'WI' : '');
  });
  if (showNotice) {
    const s = document.getElementById('donorIdleStatus');
    s.textContent = 'Donor info cleared due to inactivity';
    setTimeout(() => { s.textContent = 'Donor info clears automatically after 5 minutes of inactivity'; }, 4000);
  }
}

function resetDonorIdleTimer() {
  clearTimeout(donorIdleTimer);
  donorIdleTimer = setTimeout(() => clearDonorFields(true), DONOR_IDLE_CLEAR_MS);
}

donorFieldIds.forEach(id => {
  document.getElementById(id).addEventListener('input', resetDonorIdleTimer);
});
resetDonorIdleTimer();

document.getElementById('clearDonorBtn').addEventListener('click', () => {
  clearDonorFields(false);
  clearTimeout(donorIdleTimer); // nothing left to time out until the user types again
  toast('Donor info cleared');
});

// ---------- Scanner wiring ----------
const UNIT_OPTIONS = ['tablets', 'capsules', 'mL', 'g', 'patches', 'vials', 'ampules', 'syringes', 'pens', 'inhalers', 'films', 'strips', 'packets', 'suppositories', 'units', 'each', 'other'];

initBarcodeScanner(async (text) => {
  const parsed = parseGS1(text);
  const gtin = parsed['01'] || '';
  const lot = parsed['10'] || '';
  const expiration = formatGS1Date(parsed['17'] || '');
  let name = '', strength = '', ndcDisplay = '', quantity = '', unit = '';

  if (gtin) {
    // Fall back to the undashed raw 10 digits — honest about not knowing the
    // real labeler/product/package split — until/unless openFDA confirms one.
    ndcDisplay = gtin.length === 14 ? gtin.slice(3, 13) : '';
    const drug = await lookupDrugByGtin(gtin);
    if (drug) {
      name = drug.name; strength = drug.strength; ndcDisplay = drug.ndc;
      quantity = drug.quantity || '';
      unit = drug.unit || '';
      toast(unit ? `Found: ${name}` : `Found: ${name} — pick a unit for quantity`, 3000);
    } else {
      toast('Scanned OK, but no openFDA match — fill in name/strength/unit manually', 3200);
    }
  } else {
    toast('Could not read GTIN from barcode — check item manually');
  }

  // Hard block: a drug that is expired or expires within 90 days of the
  // donation date is not donatable under DHS 148.06(2)(c), so it is never
  // added to the record — the scan is rejected with an explanation.
  const scanExpStatus = expirationStatus(expiration);
  if (scanExpStatus) {
    const label = name || 'Item';
    toast(scanExpStatus === 'expired'
      ? `Not added — ${label} is already expired and cannot be donated`
      : `Not added — ${label} (exp ${expiration}) is within 90 days of expiring and cannot be donated per DHS 148.06(2)(c)`,
      6000);
    return;
  }

  addItem({ name, strength, ndc: ndcDisplay, lot, expiration, quantity, unit });
});

// ---------- Expiration decision support (90-day rule) ----------
// The WI Drug Repository Program (Wis. Admin. Code DHS 148.06(2)(c)) does not
// allow donating drugs that expire within 90 days of the donation date. Dates
// here are free-typed or scanned, easy to get wrong/overlook in a long item
// list, so this is enforced as a hard block: a flagged item is rejected on
// scan, cleared back out if typed/edited manually (on blur), and can never
// appear on a generated record. Only a real M/D/YYYY date is evaluated — a
// blank or unparseable date isn't blocked, so an odd format can't make the
// tool impossible to use, but a clearly in-window date is always refused.
const EXPIRY_WINDOW_DAYS = 90;

function parseUSDate(str) {
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec((str || '').trim());
  if (!m) return null;
  const mm = +m[1], dd = +m[2], yyyy = +m[3];
  const d = new Date(yyyy, mm - 1, dd);
  if (d.getMonth() !== mm - 1 || d.getDate() !== dd) return null; // rejects e.g. 02/31
  return d;
}

function donationBaseDate() {
  const iso = document.getElementById('dateDonated').value;
  if (!iso) return new Date();
  const [y, m, d] = iso.split('-').map(Number);
  return new Date(y, m - 1, d);
}

// Returns null (no issue), 'expired', or 'soon' (inside the 90-day window).
function expirationStatus(expStr) {
  const exp = parseUSDate(expStr);
  if (!exp) return null;
  const base = donationBaseDate();
  const cutoff = new Date(base);
  cutoff.setDate(cutoff.getDate() + EXPIRY_WINDOW_DAYS);
  if (exp < base) return 'expired';
  if (exp < cutoff) return 'soon';
  return null;
}

function expirationWarningText(status, expStr) {
  if (status === 'expired') return '⚠ Already expired';
  if (status === 'soon') {
    const days = Math.round((parseUSDate(expStr) - donationBaseDate()) / 86400000);
    return `⚠ Expires in ${days}d — inside 90-day window, not donatable`;
  }
  return '';
}

function updateExpirationWarning(idx) {
  const input = document.querySelector(`#itemsTableWrap [data-field="expiration"][data-idx="${idx}"]`);
  if (!input) return;
  const status = expirationStatus(state.items[idx].expiration);
  input.classList.remove('dn-exp-warn', 'dn-exp-expired', 'dn-exp-soon');
  let warnDiv = input.nextElementSibling;
  if (!warnDiv || !warnDiv.classList.contains('dn-exp-warn-text')) {
    warnDiv = document.createElement('div');
    warnDiv.className = 'dn-exp-warn-text';
    input.after(warnDiv);
  }
  if (status) {
    input.classList.add('dn-exp-warn', 'dn-exp-' + status);
    warnDiv.textContent = expirationWarningText(status, state.items[idx].expiration);
  } else {
    warnDiv.textContent = '';
  }
}

// Changing the donation date shifts the 90-day cutoff for every row.
document.getElementById('dateDonated').addEventListener('input', () => {
  state.items.forEach((_, idx) => updateExpirationWarning(idx));
});

// ---------- Items table ----------
function addItem(item) {
  state.items.push(item);
  renderItems();
}

document.getElementById('addManualBtn').addEventListener('click', () => {
  preserveScroll(() => addItem({ name: '', strength: '', ndc: '', lot: '', expiration: '', quantity: '', unit: '' }));
});

function unitSelectHtml(idx, selected) {
  const opts = UNIT_OPTIONS.map(u =>
    `<option value="${u}" ${u === selected ? 'selected' : ''}>${u}</option>`
  ).join('');
  return `<select class="dn-unit-select" data-idx="${idx}" data-field="unit">
    <option value="" ${selected ? '' : 'selected'} disabled>unit&hellip;</option>${opts}
  </select>`;
}

// Shown as a permanent, non-editable first row so a patient filling this out
// (by hand or reading over someone's shoulder) has a worked example of what a
// completed line looks like, without it ever being mistaken for a real item —
// it's excluded from state.items, so it's never counted, validated, or
// printed on the generated record.
const EXAMPLE_ITEM = { name: 'Lantus', strength: '100 units/mL', ndc: '00000-1234-56', lot: 'AB1234C', expiration: '12/31/2027', quantity: '5', unit: 'pens' };

function exampleRowHtml() {
  return `<tr class="dn-example-row">
    <td class="dn-row-num" data-label="#">Ex.</td>
    <td data-label="Name"><input type="text" value="${escapeHtml(EXAMPLE_ITEM.name)}" disabled></td>
    <td data-label="Strength"><input type="text" value="${escapeHtml(EXAMPLE_ITEM.strength)}" disabled></td>
    <td data-label="NDC"><input type="text" value="${escapeHtml(EXAMPLE_ITEM.ndc)}" disabled></td>
    <td data-label="Lot"><input type="text" value="${escapeHtml(EXAMPLE_ITEM.lot)}" disabled></td>
    <td data-label="Exp"><input type="text" value="${escapeHtml(EXAMPLE_ITEM.expiration)}" disabled></td>
    <td data-label="Qty"><input type="text" value="${escapeHtml(EXAMPLE_ITEM.quantity)}" disabled></td>
    <td data-label="Unit"><input type="text" value="${escapeHtml(EXAMPLE_ITEM.unit)}" disabled></td>
    <td class="dn-remove-cell"><span class="dn-example-tag">Example</span></td>
  </tr>`;
}

function renderItems() {
  const wrap = document.getElementById('itemsTableWrap');
  let html = '<div class="dn-table-scroll"><table><thead><tr>' +
    '<th class="dn-row-num">#</th><th>Name of Drug/Supply</th><th>Strength</th><th>NDC No.</th>' +
    '<th>Lot No.</th><th>Expiration</th><th title="Number of units, not packages">Qty Donated (units)</th><th>Unit</th><th></th></tr></thead><tbody>';
  html += exampleRowHtml();
  if (state.items.length === 0) {
    html += '<tr><td colspan="9"><div class="dn-empty-state" id="emptyState">No items scanned yet</div></td></tr>';
  }
  state.items.forEach((it, idx) => {
    const expStatus = expirationStatus(it.expiration);
    const expClass = expStatus ? ` dn-exp-warn dn-exp-${expStatus}` : '';
    const expWarnText = expStatus ? expirationWarningText(expStatus, it.expiration) : '';
    html += `<tr>
      <td class="dn-row-num" data-label="#">${idx + 1}</td>
      <td data-label="Name"><input type="text" data-idx="${idx}" data-field="name" value="${escapeHtml(it.name)}"></td>
      <td data-label="Strength"><input type="text" data-idx="${idx}" data-field="strength" value="${escapeHtml(it.strength)}"></td>
      <td data-label="NDC"><input type="text" data-idx="${idx}" data-field="ndc" value="${escapeHtml(it.ndc)}"></td>
      <td data-label="Lot"><input type="text" data-idx="${idx}" data-field="lot" value="${escapeHtml(it.lot)}"></td>
      <td data-label="Exp"><input type="text" class="${expClass}" data-idx="${idx}" data-field="expiration" value="${escapeHtml(it.expiration)}"><div class="dn-exp-warn-text">${expWarnText}</div></td>
      <td data-label="Qty"><input type="text" inputmode="numeric" placeholder="e.g. 30" title="Number of units, not packages" data-idx="${idx}" data-field="quantity" value="${escapeHtml(it.quantity)}"></td>
      <td data-label="Unit">${unitSelectHtml(idx, it.unit)}</td>
      <td class="dn-remove-cell"><button class="dn-remove-btn" data-idx="${idx}" aria-label="Remove item">✕</button></td>
    </tr>`;
  });
  html += '</tbody></table></div>';
  wrap.innerHTML = html;

  wrap.querySelectorAll('input:not(:disabled)').forEach(inp => {
    inp.addEventListener('input', e => {
      const idx = +e.target.dataset.idx, field = e.target.dataset.field;
      state.items[idx][field] = e.target.value;
      if (field === 'expiration') {
        updateExpirationWarning(idx);
      }
    });
    // Hard block for a manually typed/edited expiration: on blur, if the date
    // is expired or inside the 90-day window, it is not a donatable item, so
    // the date is cleared back out and the user is told why. Checked on
    // 'change' (not 'input') so partially-typed dates aren't fought with.
    if (inp.dataset.field === 'expiration') {
      inp.addEventListener('change', e => {
        const idx = +e.target.dataset.idx;
        if (expirationStatus(state.items[idx].expiration)) {
          const bad = state.items[idx].expiration;
          state.items[idx].expiration = '';
          e.target.value = '';
          updateExpirationWarning(idx);
          toast(`Expiration ${bad} removed — a drug that is expired or expires within 90 days cannot be donated (DHS 148.06(2)(c))`, 6000);
        }
      });
    }
  });
  wrap.querySelectorAll('select.dn-unit-select').forEach(sel => {
    sel.addEventListener('change', e => {
      const idx = +e.target.dataset.idx;
      state.items[idx].unit = e.target.value;
    });
  });
  wrap.querySelectorAll('.dn-remove-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      state.items.splice(+e.target.dataset.idx, 1);
      renderItems();
    });
  });
}

function escapeHtml(s) {
  return (s || '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Render once on load so the example row (and table header) are visible
// immediately, rather than only appearing after the first item is added —
// the HTML template's static placeholder markup is just a no-JS fallback.
renderItems();

// ---------- Signature pad ----------
const canvas = document.getElementById('sigpad');
const ctx = canvas.getContext('2d');
let drawing = false, hasDrawnSig = false;
let lastRectW = 0, lastRectH = 0;

function resizeCanvas() {
  const rect = canvas.getBoundingClientRect();
  // iOS Safari fires `resize` when the address bar collapses/expands on
  // scroll, even though the canvas's CSS size hasn't changed. Re-scaling in
  // that case would wipe out a signature the user already drew, so only
  // touch the canvas when its on-screen size actually changed, and never
  // clear an existing signature.
  if (Math.round(rect.width) === lastRectW && Math.round(rect.height) === lastRectH) return;
  if (hasDrawnSig) return;
  lastRectW = Math.round(rect.width);
  lastRectH = Math.round(rect.height);
  const ratio = window.devicePixelRatio || 1;
  canvas.width = rect.width * ratio;
  canvas.height = rect.height * ratio;
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.scale(ratio, ratio);
  ctx.lineWidth = 2;
  ctx.lineCap = 'round';
  ctx.strokeStyle = '#111';
}
window.addEventListener('resize', resizeCanvas);
setTimeout(resizeCanvas, 50);

function pos(e) {
  const rect = canvas.getBoundingClientRect();
  const p = e.touches ? e.touches[0] : e;
  return { x: p.clientX - rect.left, y: p.clientY - rect.top };
}
function start(e) { drawing = true; hasDrawnSig = true; const p = pos(e); ctx.beginPath(); ctx.moveTo(p.x, p.y); updateSigStatus(); e.preventDefault(); }
function move(e) { if (!drawing) return; const p = pos(e); ctx.lineTo(p.x, p.y); ctx.stroke(); e.preventDefault(); }
function end() { drawing = false; }
canvas.addEventListener('mousedown', start);
canvas.addEventListener('mousemove', move);
window.addEventListener('mouseup', end);
canvas.addEventListener('touchstart', start, { passive: false });
canvas.addEventListener('touchmove', move, { passive: false });
canvas.addEventListener('touchend', end);

// ---------- Signature mode (draw vs. typed) ----------
// The signature is optional (no hard block at Generate time) — a typed name
// is rendered in a script font on the printed record in place of an <img>.
const sigTypedInput = document.getElementById('sigTypedName');
const sigTypedPreview = document.getElementById('sigTypedPreview');
let sigMode = 'draw';

function hasSignature() {
  return sigMode === 'draw' ? hasDrawnSig : sigTypedInput.value.trim().length > 0;
}

function updateSigStatus() {
  const statusEl = document.getElementById('sigStatus');
  if (hasSignature()) {
    statusEl.textContent = 'Signed';
    statusEl.style.color = '#1a7a3a';
    document.querySelector('.dn-sig-pad-wrap')?.classList.remove('field-missing');
    document.getElementById('sigTypeWrap')?.classList.remove('field-missing');
  } else {
    statusEl.textContent = 'No signature (optional)';
    statusEl.style.color = '#888';
  }
}

function setSigMode(mode) {
  sigMode = mode;
  document.getElementById('sigModeDrawBtn').classList.toggle('active', mode === 'draw');
  document.getElementById('sigModeTypeBtn').classList.toggle('active', mode === 'type');
  document.getElementById('sigDrawWrap').style.display = mode === 'draw' ? '' : 'none';
  document.getElementById('sigTypeWrap').style.display = mode === 'type' ? '' : 'none';
  updateSigStatus();
}

document.getElementById('sigModeDrawBtn').addEventListener('click', () => setSigMode('draw'));
document.getElementById('sigModeTypeBtn').addEventListener('click', () => setSigMode('type'));
sigTypedInput.addEventListener('input', () => {
  sigTypedPreview.textContent = sigTypedInput.value;
  updateSigStatus();
});

document.getElementById('clearSigBtn').addEventListener('click', () => {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  hasDrawnSig = false;
  sigTypedInput.value = '';
  sigTypedPreview.textContent = '';
  updateSigStatus();
});

// ---------- Reset ----------
document.getElementById('resetBtn').addEventListener('click', () => {
  if (!confirm('Clear all scanned items and the signature? Recipient info is kept, donor info is cleared.')) return;
  preserveScroll(() => {
    // Clear any red "missing field" highlight left over from a prior failed
    // Generate attempt — without this, Reset could look like it did nothing
    // if the only visible symptom was a highlighted header field.
    document.querySelectorAll('.field-missing').forEach(el => el.classList.remove('field-missing'));
    state.items = [];
    renderItems();
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    hasDrawnSig = false;
    sigTypedInput.value = '';
    sigTypedPreview.textContent = '';
    updateSigStatus();
    clearDonorFields(false);
    clearTimeout(donorIdleTimer);
  });
});

// ---------- Generate printable official record ----------
// Required fields, checked top-to-bottom to match reading order — the first
// missing one gets scrolled to, highlighted, and named, rather than silently
// letting the record print with a blank field.
const REQUIRED_FIELDS = [
  ['donorName', 'the donor name'],
  ['donorAddress', 'the donor street address'],
  ['donorCity', 'the donor city'],
  ['donorState', 'the donor state'],
  ['donorZip', 'the donor zip code'],
  ['dateDonated', 'the date donated'],
];

// Shared by both Generate & Print and Send to Cognito — both actions require
// the record to be in the same valid, complete state. Returns false (having
// already shown a toast/highlight naming the problem) if anything is wrong,
// true otherwise.
function validateRecordOrWarn() {
  for (const [id, label] of REQUIRED_FIELDS) {
    const el = document.getElementById(id);
    if (!el.value) { warnMissingField(el, `Enter ${label} before generating the record`); return false; }
  }
  if (!getRecipientName()) {
    const el = document.getElementById('recipientName').value === '__other__'
      ? document.getElementById('recipientNameOther')
      : document.getElementById('recipientName');
    warnMissingField(el, 'Select or enter the recipient facility name before generating the record');
    return false;
  }
  if (state.items.length === 0) { toast('Add at least one item before generating the record'); return false; }
  for (let i = 0; i < state.items.length; i++) {
    if (!state.items[i].quantity) {
      const el = document.querySelector(`#itemsTableWrap [data-field="quantity"][data-idx="${i}"]`);
      warnMissingField(el, `Enter a quantity for item ${i + 1} before generating the record`);
      return false;
    }
    if (!state.items[i].unit) {
      const el = document.querySelector(`#itemsTableWrap [data-field="unit"][data-idx="${i}"]`);
      warnMissingField(el, `Pick a unit for item ${i + 1} before generating the record`);
      return false;
    }
  }
  // Hard block (safety net — flagged items are already rejected on scan and on
  // manual entry, but the donation date can be moved later, pulling an
  // already-added item into the window). A record can never be generated with
  // a flagged item; the user must remove it.
  const flaggedIdx = state.items
    .map((it, i) => (expirationStatus(it.expiration) ? i : -1))
    .filter(i => i >= 0);
  if (flaggedIdx.length > 0) {
    const firstEl = document.querySelector(`#itemsTableWrap [data-field="expiration"][data-idx="${flaggedIdx[0]}"]`);
    firstEl?.scrollIntoView({ behavior: 'smooth', block: 'center' });
    const itemList = flaggedIdx.map(i => `#${i + 1}`).join(', ');
    const anyExpired = flaggedIdx.some(i => expirationStatus(state.items[i].expiration) === 'expired');
    warnMissingField(firstEl, `Item${flaggedIdx.length > 1 ? 's' : ''} ${itemList} ${anyExpired ? 'is expired or expires' : 'expires'} within 90 days of the donation date and cannot be donated (DHS 148.06(2)(c)). Remove ${flaggedIdx.length > 1 ? 'them' : 'it'} to generate the record.`);
    return false;
  }
  // The signature itself is optional (see hasSignature() above) — only
  // require a signed date to go with it when there's an actual signature to
  // date. Without this, someone printing an unsigned record (e.g. to sign by
  // hand later) could get blocked by a date field for a signature that was
  // never going to be there.
  const dateSignedEl = document.getElementById('dateSigned');
  if (hasSignature() && !dateSignedEl.value) { warnMissingField(dateSignedEl, 'Enter the date signed before generating the record'); return false; }
  return true;
}

// Reads the current form state into the shape buildFormPage()/buildPdfDocDefinition()
// need. Assumes validateRecordOrWarn() has already passed.
function collectRecordData() {
  const donor = {
    name: document.getElementById('donorName').value,
    address: document.getElementById('donorAddress').value,
    city: document.getElementById('donorCity').value,
    state: document.getElementById('donorState').value,
    zip: document.getElementById('donorZip').value,
    dateDonatedIso: document.getElementById('dateDonated').value,
    dateSignedIso: document.getElementById('dateSigned').value,
    dateDonated: fmtDate(document.getElementById('dateDonated').value),
    dateSigned: fmtDate(document.getElementById('dateSigned').value),
    recipientName: getRecipientName(),
  };
  const sig = !hasSignature() ? { type: 'none' }
    : sigMode === 'draw' ? { type: 'draw', dataUrl: canvas.toDataURL('image/png') }
    : { type: 'type', text: sigTypedInput.value.trim() };

  const pages = [];
  for (let i = 0; i < state.items.length; i += 10) pages.push(state.items.slice(i, i + 10));

  return { donor, sig, pages, items: state.items };
}

document.getElementById('generateBtn').addEventListener('click', () => {
  if (!validateRecordOrWarn()) return;

  preserveScroll(async () => {
    const { donor, sig, pages } = collectRecordData();

    // Captured once, outside the loop, so every page's footer shows the same
    // "generated at" instant rather than drifting across however long
    // buildFormPage()/print take to run for a large multi-page donation.
    const generatedAt = new Date();

    let html = '';
    pages.forEach((pageItems, i) => {
      html += buildFormPage(donor, pageItems, sig, i + 1, pages.length, generatedAt);
    });
    const printArea = document.getElementById('print-area');
    printArea.innerHTML = html;
    // Wait for the signature <img> to actually finish decoding — otherwise
    // printing immediately after setting a large data-URL src can race the
    // browser's decode and produce a blank spot where the signature should be.
    await waitForImagesToDecode(printArea);

    if (isIOS) {
      toast('Opening print preview — use the Share icon, then "Save to Files" for a PDF', 5000);
    }
    window.print();

    // Donor info is patient-identifying — clear it from memory immediately
    // after the record is generated. From this point on, the printed/saved
    // output is the durable record; the app's responsibility for this data
    // ends here. Handle that output per SVdP's standard PHI procedures
    // (secure storage, controlled disposal) — this app does not store or
    // transmit donor data itself.
    clearDonorFields(false);
    clearTimeout(donorIdleTimer);
    toast('Record generated and donor info cleared from this page. The printed/saved file now contains donor information — handle it per SVdP’s PHI procedures.', 6000);
  });
});

function fmtDate(iso) {
  if (!iso) return '';
  const [y, m, d] = iso.split('-');
  return `${m}/${d}/${y}`;
}

function signatureLineHtml(sig) {
  if (sig.type === 'draw') return `<img src="${sig.dataUrl}" alt="signature">`;
  if (sig.type === 'type') return `<span class="dn-sig-typed-print">${escapeHtml(sig.text)}</span>`;
  return '';
}

function fmtDateTime(d) {
  const date = `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
  let h = d.getHours();
  const ampm = h >= 12 ? 'PM' : 'AM';
  h = h % 12 || 12;
  const m = String(d.getMinutes()).padStart(2, '0');
  return `${date} ${h}:${m} ${ampm}`;
}

// A multi-item donation is split across several of these pages (10 items
// each, see the caller) — only the LAST page carries the Attestation and
// Signature sections, so the donor signs once for the whole donation rather
// than once per page. Every page still gets its own footer (donation date,
// when the record was generated, and "Page X of Y") so a stack of printed
// pages — possibly interleaved with other donors' multi-page records — can
// always be sorted back into the right donation and see how many pages
// belong to it.
function buildFormPage(f, items, sig, pageNum, totalPages, generatedAt) {
  const isLastPage = pageNum === totalPages;
  let rows = '';
  items.forEach(it => {
    rows += `<tr>
      <td>${escapeHtml(it.name)}</td>
      <td>${escapeHtml(it.strength)}</td>
      <td>${escapeHtml(it.ndc)}</td>
      <td>${escapeHtml(it.lot)}</td>
      <td>${escapeHtml(it.expiration)}</td>
      <td>${escapeHtml(it.quantity)} ${escapeHtml(it.unit)}</td>
    </tr>`;
  });

  const attestationAndSignature = isLastPage ? `
    <div class="dn-section-header">Attestation</div>
    <table class="dn-official">
      <tr><td style="font-size:9.5px;">I attest that the drugs or medical supplies listed on this record${totalPages > 1 ? ` (pages 1&ndash;${totalPages})` : ''} were stored as recommended by the manufacturer and have not been subject to tampering.</td></tr>
    </table>

    <div class="dn-section-header">Signature</div>
    <table class="dn-official">
      <tr>
        <td style="width:30%;"><span class="dn-label">Date Signed (MM/dd/yyyy)</span><span class="dn-value">${f.dateSigned}</span></td>
        <td style="width:70%;">
          <span class="dn-label">Signature &ndash; Donor</span>
          <div class="dn-sig-line">${signatureLineHtml(sig)}</div>
        </td>
      </tr>
    </table>` : '';

  return `
  <div class="dn-form-page">
    <div class="dn-form-title-block">
      <span class="dn-formnum">F-62645B (05/2020)</span>
      <div class="dn-agency">DEPARTMENT OF HEALTH SERVICES STATE OF WISCONSIN</div>
      <div class="dn-division">Division of Quality Assurance</div>
      <div class="dn-title">Drug Repository Program &mdash; Donation Record</div>
    </div>
    <ul class="dn-form-note">
      <li>Completion of this form meets the requirements of Wisconsin Administrative Code &sect;&sect; DHS 148.06(2)(a)1 and (b)3 for donating drugs and medical supplies.</li>
      <li>Questions about completion of this form may be directed to 608-266-5388.</li>
    </ul>

    <div class="dn-section-header">Donor Information</div>
    <table class="dn-official">
      <tr>
        <td style="width:70%;"><span class="dn-label">Name &ndash; Donor</span><span class="dn-value">${escapeHtml(f.name)}</span></td>
        <td style="width:30%;"><span class="dn-label">Date Donated (MM/dd/yyyy)</span><span class="dn-value">${f.dateDonated}</span></td>
      </tr>
      <tr>
        <td><span class="dn-label">Street Address</span><span class="dn-value">${escapeHtml(f.address)}</span></td>
        <td><span class="dn-label">City / State / Zip</span><span class="dn-value">${escapeHtml(f.city)}, ${escapeHtml(f.state)} ${escapeHtml(f.zip)}</span></td>
      </tr>
    </table>

    <div class="dn-section-header">Recipient Information</div>
    <table class="dn-official">
      <tr>
        <td><span class="dn-label">Name &ndash; Pharmacy or Medical Facility Receiving Donations</span><span class="dn-value">${escapeHtml(f.recipientName)}</span></td>
      </tr>
    </table>

    <div class="dn-section-header">Drug / Medical Supply Information</div>
    <table class="dn-official dn-drug-table">
      <thead>
        <tr>
          <th>Name of Drug or Medical Supply</th><th>Strength</th><th>NDC No.</th>
          <th>Lot No.</th><th>Expiration Date</th><th>Quantity Donated</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    ${attestationAndSignature}

    <div class="dn-form-footer">
      <span>Date Donated: ${f.dateDonated}</span>
      <span>Generated: ${fmtDateTime(generatedAt)}</span>
      <span>Page ${pageNum} of ${totalPages}</span>
    </div>
  </div>`;
}

// ---------- Send to Cognito (client-rendered PDF via vendored pdfmake) ----------
// pdfmake draws text as real PDF text objects (not a rasterized screenshot),
// so it stays crisp at any zoom/DPI — unlike an html2canvas-based approach,
// which would blur on high-density phone screens. This mirrors buildFormPage()'s
// structure/content but as a pdfmake docDefinition rather than HTML.
const PDF_GRID_LAYOUT = {
  hLineWidth: () => 1,
  vLineWidth: () => 1,
  hLineColor: () => '#000',
  vLineColor: () => '#000',
  paddingLeft: () => 6,
  paddingRight: () => 6,
  paddingTop: () => 4,
  paddingBottom: () => 4,
};

function pdfSectionHeader(label) {
  return {
    table: { widths: ['100%'], body: [[{ text: label.toUpperCase(), style: 'sectionHeader' }]] },
    layout: PDF_GRID_LAYOUT,
    margin: [0, 4, 0, 4],
  };
}

function pdfFieldCell(label, value) {
  return { stack: [{ text: label, style: 'fieldLabel' }, { text: value || ' ', style: 'fieldValue' }] };
}

function buildPdfPageContent(f, items, sig, pageNum, totalPages, generatedAt) {
  const isLastPage = pageNum === totalPages;
  const content = [];
  if (pageNum > 1) content.push({ text: '', pageBreak: 'before' });

  content.push({
    columns: [
      { text: 'DEPARTMENT OF HEALTH SERVICES STATE OF WISCONSIN', style: 'agency' },
      { text: 'F-62645B (05/2020)', style: 'formNum' },
    ],
  });
  content.push({ text: 'Division of Quality Assurance', style: 'division', margin: [0, 0, 0, 6] });
  content.push({ text: 'DRUG REPOSITORY PROGRAM — DONATION RECORD', style: 'title' });
  content.push({
    ul: [
      'Completion of this form meets the requirements of Wisconsin Administrative Code §§ DHS 148.06(2)(a)1 and (b)3 for donating drugs and medical supplies.',
      'Questions about completion of this form may be directed to 608-266-5388.',
    ],
    style: 'note',
    margin: [0, 0, 0, 8],
  });

  content.push(pdfSectionHeader('Donor Information'));
  content.push({
    table: {
      widths: ['70%', '30%'],
      body: [
        [pdfFieldCell('Name – Donor', f.name), pdfFieldCell('Date Donated (MM/DD/YYYY)', f.dateDonated)],
        [pdfFieldCell('Street Address', f.address), pdfFieldCell('City / State / Zip', `${f.city}, ${f.state} ${f.zip}`)],
      ],
    },
    layout: PDF_GRID_LAYOUT,
    margin: [0, 0, 0, 8],
  });

  content.push(pdfSectionHeader('Recipient Information'));
  content.push({
    table: { widths: ['100%'], body: [[pdfFieldCell('Name – Pharmacy or Medical Facility Receiving Donations', f.recipientName)]] },
    layout: PDF_GRID_LAYOUT,
    margin: [0, 0, 0, 8],
  });

  content.push(pdfSectionHeader('Drug / Medical Supply Information'));
  const itemRows = items.map(it => ([
    { text: it.name || '', style: 'tableCell' },
    { text: it.strength || '', style: 'tableCell' },
    { text: it.ndc || '', style: 'tableCell' },
    { text: it.lot || '', style: 'tableCell' },
    { text: it.expiration || '', style: 'tableCell' },
    { text: `${it.quantity || ''} ${it.unit || ''}`.trim(), style: 'tableCell' },
  ]));
  content.push({
    table: {
      headerRows: 1,
      widths: ['26%', '14%', '14%', '12%', '14%', '20%'],
      body: [
        [
          { text: 'Name of Drug or Medical Supply', style: 'tableHeader' },
          { text: 'Strength', style: 'tableHeader' },
          { text: 'NDC No.', style: 'tableHeader' },
          { text: 'Lot No.', style: 'tableHeader' },
          { text: 'Expiration Date', style: 'tableHeader' },
          { text: 'Quantity Donated', style: 'tableHeader' },
        ],
        ...itemRows,
      ],
    },
    layout: PDF_GRID_LAYOUT,
    margin: [0, 0, 0, 8],
  });

  if (isLastPage) {
    content.push(pdfSectionHeader('Attestation'));
    const attestationText = `I attest that the drugs or medical supplies listed on this record${totalPages > 1 ? ` (pages 1–${totalPages})` : ''} were stored as recommended by the manufacturer and have not been subject to tampering.`;
    content.push({
      table: { widths: ['100%'], body: [[{ text: attestationText, fontSize: 8 }]] },
      layout: PDF_GRID_LAYOUT,
      margin: [0, 0, 0, 8],
    });

    content.push(pdfSectionHeader('Signature'));
    // pdfmake's bundled fonts don't include a cursive/script face, so a typed
    // signature renders in italic body text here rather than the
    // 'Brush Script MT'-style look the printed/HTML record uses — a known,
    // acceptable cosmetic difference since the legal content (the name and
    // date) is identical either way. A drawn signature embeds the actual
    // canvas PNG, so it matches Print exactly.
    const sigCellContent = sig.type === 'draw'
      ? { image: sig.dataUrl, width: 140, height: 40 }
      : sig.type === 'type'
        ? { text: sig.text, italics: true, fontSize: 16 }
        : { text: ' ' };
    content.push({
      table: {
        widths: ['30%', '70%'],
        body: [[
          pdfFieldCell('Date Signed (MM/DD/YYYY)', f.dateSigned),
          { stack: [{ text: 'Signature – Donor', style: 'fieldLabel' }, sigCellContent] },
        ]],
      },
      layout: PDF_GRID_LAYOUT,
      margin: [0, 0, 0, 8],
    });
  }

  content.push({
    columns: [
      { text: `Date Donated: ${f.dateDonated}`, style: 'footer' },
      { text: `Generated: ${fmtDateTime(generatedAt)}`, style: 'footer', alignment: 'center' },
      { text: `Page ${pageNum} of ${totalPages}`, style: 'footer', alignment: 'right' },
    ],
    margin: [0, 6, 0, 0],
  });

  return content;
}

function buildPdfDocDefinition(donor, pages, sig, generatedAt) {
  let content = [];
  pages.forEach((pageItems, i) => {
    content = content.concat(buildPdfPageContent(donor, pageItems, sig, i + 1, pages.length, generatedAt));
  });
  return {
    pageSize: 'LETTER',
    pageMargins: [28, 28, 28, 28],
    defaultStyle: { fontSize: 9.5 },
    content,
    styles: {
      formNum: { fontSize: 8, alignment: 'right' },
      agency: { fontSize: 11, bold: true },
      division: { fontSize: 10 },
      title: { fontSize: 14, bold: true, alignment: 'center', margin: [0, 8, 0, 8] },
      note: { fontSize: 8 },
      sectionHeader: { fontSize: 9, bold: true, alignment: 'center', fillColor: '#dde5ec' },
      fieldLabel: { fontSize: 6.5, bold: true, color: '#333' },
      fieldValue: { fontSize: 9.5 },
      tableHeader: { fontSize: 8, bold: true, alignment: 'center', fillColor: '#eef2f6' },
      tableCell: { fontSize: 8.5 },
      footer: { fontSize: 7, color: '#555' },
    },
  };
}

function generateDonationPdfBlob(donor, pages, sig, generatedAt) {
  return new Promise((resolve, reject) => {
    try {
      pdfMake.createPdf(buildPdfDocDefinition(donor, pages, sig, generatedAt)).getBlob(resolve);
    } catch (e) {
      reject(e);
    }
  });
}

function toIsoDate(mmddyyyy) {
  const d = parseUSDate(mmddyyyy);
  if (!d) return '';
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${d.getFullYear()}-${mm}-${dd}`;
}

const sendBtn = document.getElementById('sendBtn');
sendBtn.addEventListener('click', () => {
  if (!validateRecordOrWarn()) return;

  preserveScroll(async () => {
    const originalLabel = sendBtn.textContent;
    sendBtn.disabled = true;
    sendBtn.innerHTML = '<span class="dn-spinner"></span>Sending…';

    const { donor, sig, pages, items } = collectRecordData();
    const generatedAt = new Date();
    let sendAttempted = false;
    try {
      const pdfBlob = await generateDonationPdfBlob(donor, pages, sig, generatedAt);

      const fields = {
        donor_name: donor.name,
        donor_street_address: donor.address,
        donor_city: donor.city,
        donor_state: donor.state,
        donor_zip_code: donor.zip,
        recipient_facility: donor.recipientName,
        date_donated: donor.dateDonatedIso,
        date_signed: donor.dateSignedIso || '',
        items: items.map(it => ({
          drug_name: it.name,
          strength: it.strength || '',
          ndc: it.ndc || '',
          lot_number: it.lot || '',
          expiration_date: toIsoDate(it.expiration) || '',
          quantity: parseFloat(it.quantity) || 0,
          unit: it.unit || '',
        })),
      };

      const formData = new FormData();
      formData.append('record', pdfBlob, 'donation-record.pdf');
      formData.append('fields', JSON.stringify(fields));

      sendAttempted = true;
      const res = await fetch('/api/donation-submit', { method: 'POST', body: formData });
      if (!res.ok) {
        let detail = 'Could not send the record.';
        try {
          const body = await res.json();
          if (body.detail) detail = body.detail;
        } catch (e) { /* non-JSON error body, keep generic message */ }
        throw new Error(detail);
      }

      toast('Sent to Cognito and donor info cleared from this page.', 6000);
    } catch (e) {
      const msg = sendAttempted
        ? (e.message || 'Could not send the record.')
        : 'Could not build the PDF to send.';
      toast(`${msg} Use Print instead.`, 7000);
    } finally {
      // Donor info is patient-identifying — clear it regardless of outcome,
      // same policy as Generate & Print (see that handler's comment). A
      // failed send still attempted to transmit it, so there's no safer
      // "keep it around to retry" state to preserve.
      clearDonorFields(false);
      clearTimeout(donorIdleTimer);
      sendBtn.disabled = false;
      sendBtn.textContent = originalLabel;
    }
  });
});
