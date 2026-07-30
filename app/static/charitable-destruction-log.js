// Charitable Pharmacy Destruction Log — client-side only, for medications
// from charitable donation programs (Dispensary of Hope, Americares, etc.)
// that are separate from the Wisconsin Drug Repository Program. GS1 parsing,
// GTIN->NDC lookup, openFDA lookup, and camera/keyboard-wedge scanning live
// in the shared gs1-scanner.js (loaded before this file).
//
// No patient/donor data here — facility and drug info only, same as the
// Drug Repository Destruction Record — so none of the PHI-minimizing
// patterns used on the Drug Donation Record tool apply to this page.

const state = {
  items: [], // {name, strength, ndc, lot, expiration, quantity, unit}
};

const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) ||
  (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);

// ---------- Persistence for facility defaults ----------
// Source Program is intentionally NOT persisted — it should be chosen
// explicitly each time to avoid accidentally mislabeling a record.
const persistFields = ['facilityName', 'facilityAddress', 'facilityCity', 'facilityState', 'facilityZip'];
persistFields.forEach(id => {
  const el = document.getElementById(id);
  const saved = localStorage.getItem('charitablelog_' + id);
  if (saved) el.value = saved;
  el.addEventListener('change', () => localStorage.setItem('charitablelog_' + id, el.value));
});

(function setDefaultDates() {
  const today = new Date().toISOString().slice(0, 10);
  document.getElementById('dateDestroyed').value = today;
  document.getElementById('dateSigned').value = today;
})();

if (isIOS) {
  const hint = document.getElementById('iosPrintHint');
  if (hint) hint.style.display = 'block';
}

// ---------- Scanner wiring ----------
const UNIT_OPTIONS = ['tablets', 'capsules', 'mL', 'patches', 'vials', 'packets', 'suppositories', 'units', 'each', 'other'];

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

  addItem({ name, strength, ndc: ndcDisplay, lot, expiration, quantity, unit });
});

// ---------- Items table ----------
function addItem(item) {
  state.items.push(item);
  renderItems();
}

document.getElementById('addManualBtn').addEventListener('click', () => {
  addItem({ name: '', strength: '', ndc: '', lot: '', expiration: '', quantity: '', unit: '' });
});

function unitSelectHtml(idx, selected) {
  const opts = UNIT_OPTIONS.map(u =>
    `<option value="${u}" ${u === selected ? 'selected' : ''}>${u}</option>`
  ).join('');
  return `<select class="cl-unit-select ${selected ? '' : 'cl-qty-empty'}" data-idx="${idx}" data-field="unit">
    <option value="" ${selected ? '' : 'selected'} disabled>unit&hellip;</option>${opts}
  </select>`;
}

function renderItems() {
  const wrap = document.getElementById('itemsTableWrap');
  if (state.items.length === 0) {
    wrap.innerHTML = '<div class="cl-empty-state" id="emptyState">No items scanned yet</div>';
    return;
  }
  let html = '<div class="cl-table-scroll"><table><thead><tr>' +
    '<th class="cl-row-num">#</th><th>Name of Drug/Supply</th><th>Strength</th><th>NDC No.</th>' +
    '<th>Lot No.</th><th>Expiration</th><th title="Number of units, not packages">Qty Destroyed (units)</th><th>Unit</th><th></th></tr></thead><tbody>';
  state.items.forEach((it, idx) => {
    html += `<tr>
      <td class="cl-row-num" data-label="#">${idx + 1}</td>
      <td data-label="Name"><input type="text" data-idx="${idx}" data-field="name" value="${escapeHtml(it.name)}"></td>
      <td data-label="Strength"><input type="text" data-idx="${idx}" data-field="strength" value="${escapeHtml(it.strength)}"></td>
      <td data-label="NDC"><input type="text" data-idx="${idx}" data-field="ndc" value="${escapeHtml(it.ndc)}"></td>
      <td data-label="Lot"><input type="text" data-idx="${idx}" data-field="lot" value="${escapeHtml(it.lot)}"></td>
      <td data-label="Exp"><input type="text" data-idx="${idx}" data-field="expiration" value="${escapeHtml(it.expiration)}"></td>
      <td data-label="Qty"><input type="text" inputmode="numeric" placeholder="e.g. 30" title="Number of units, not packages" class="${it.quantity ? '' : 'cl-qty-empty'}" data-idx="${idx}" data-field="quantity" value="${escapeHtml(it.quantity)}"></td>
      <td data-label="Unit">${unitSelectHtml(idx, it.unit)}</td>
      <td class="cl-remove-cell"><button class="cl-remove-btn" data-idx="${idx}" aria-label="Remove item">✕</button></td>
    </tr>`;
  });
  html += '</tbody></table></div>';
  wrap.innerHTML = html;

  wrap.querySelectorAll('input').forEach(inp => {
    inp.addEventListener('input', e => {
      const idx = +e.target.dataset.idx, field = e.target.dataset.field;
      state.items[idx][field] = e.target.value;
      if (field === 'quantity') {
        e.target.classList.toggle('cl-qty-empty', !e.target.value);
      }
    });
  });
  wrap.querySelectorAll('select.cl-unit-select').forEach(sel => {
    sel.addEventListener('change', e => {
      const idx = +e.target.dataset.idx;
      state.items[idx].unit = e.target.value;
      e.target.classList.toggle('cl-qty-empty', !e.target.value);
    });
  });
  wrap.querySelectorAll('.cl-remove-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      state.items.splice(+e.target.dataset.idx, 1);
      renderItems();
    });
  });
}

function escapeHtml(s) {
  return (s || '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// ---------- Signature pad ----------
const canvas = document.getElementById('sigpad');
const ctx = canvas.getContext('2d');
let drawing = false, hasSig = false;
let lastRectW = 0, lastRectH = 0;

function resizeCanvas() {
  const rect = canvas.getBoundingClientRect();
  // iOS Safari fires `resize` when the address bar collapses/expands on
  // scroll, even though the canvas's CSS size hasn't changed. Re-scaling in
  // that case would wipe out a signature the user already drew, so only
  // touch the canvas when its on-screen size actually changed, and never
  // clear an existing signature.
  if (Math.round(rect.width) === lastRectW && Math.round(rect.height) === lastRectH) return;
  if (hasSig) return;
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
function start(e) { drawing = true; hasSig = true; const p = pos(e); ctx.beginPath(); ctx.moveTo(p.x, p.y); document.getElementById('sigStatus').textContent = 'Signed'; document.getElementById('sigStatus').style.color = '#1a7a3a'; e.preventDefault(); }
function move(e) { if (!drawing) return; const p = pos(e); ctx.lineTo(p.x, p.y); ctx.stroke(); e.preventDefault(); }
function end() { drawing = false; }
canvas.addEventListener('mousedown', start);
canvas.addEventListener('mousemove', move);
window.addEventListener('mouseup', end);
canvas.addEventListener('touchstart', start, { passive: false });
canvas.addEventListener('touchmove', move, { passive: false });
canvas.addEventListener('touchend', end);

document.getElementById('clearSigBtn').addEventListener('click', () => {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  hasSig = false;
  document.getElementById('sigStatus').textContent = 'Not signed';
  document.getElementById('sigStatus').style.color = '#c0392b';
});

// ---------- Reset ----------
document.getElementById('resetBtn').addEventListener('click', () => {
  if (!confirm('Clear all scanned items and the signature? Facility info is kept.')) return;
  state.items = [];
  renderItems();
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  hasSig = false;
  document.getElementById('sigStatus').textContent = 'Not signed';
  document.getElementById('sigStatus').style.color = '#c0392b';
});

// ---------- Generate printable log ----------
document.getElementById('generateBtn').addEventListener('click', () => {
  if (state.items.length === 0) { toast('Add at least one item before generating the record'); return; }
  const missingQty = state.items.some(it => !it.quantity);
  if (missingQty) { toast('Enter a quantity for every item before generating'); return; }
  const missingUnit = state.items.some(it => !it.unit);
  if (missingUnit) { toast('Pick a unit (tablets, mL, etc.) for every item before generating'); return; }
  if (!hasSig) { toast('Signature is required before generating the record'); return; }
  const sourceProgram = document.getElementById('sourceProgram').value;
  if (!sourceProgram) { toast('Select a source program before generating the record'); return; }

  const facility = {
    sourceProgram,
    name: document.getElementById('facilityName').value,
    address: document.getElementById('facilityAddress').value,
    city: document.getElementById('facilityCity').value,
    state: document.getElementById('facilityState').value,
    zip: document.getElementById('facilityZip').value,
    person: document.getElementById('personDestroying').value,
    dateDestroyed: fmtDate(document.getElementById('dateDestroyed').value),
    dateSigned: fmtDate(document.getElementById('dateSigned').value),
  };
  const sigDataUrl = canvas.toDataURL('image/png');

  const pages = [];
  for (let i = 0; i < state.items.length; i += 10) pages.push(state.items.slice(i, i + 10));

  let html = '';
  pages.forEach((pageItems, pIdx) => {
    html += buildFormPage(facility, pageItems, sigDataUrl, pIdx === pages.length - 1);
  });
  document.getElementById('print-area').innerHTML = html;

  if (isIOS) {
    toast('Opening print preview — use the Share icon, then "Save to Files" for a PDF', 5000);
  }
  window.print();
});

function fmtDate(iso) {
  if (!iso) return '';
  const [y, m, d] = iso.split('-');
  return `${m}/${d}/${y}`;
}

function buildFormPage(f, items, sigDataUrl, isLastPage) {
  let rows = '';
  for (let r = 0; r < 10; r++) {
    const it = items[r];
    rows += `<tr>
      <td>${it ? escapeHtml(it.name) : ''}</td>
      <td>${it ? escapeHtml(it.strength) : ''}</td>
      <td>${it ? escapeHtml(it.ndc) : ''}</td>
      <td>${it ? escapeHtml(it.lot) : ''}</td>
      <td>${it ? escapeHtml(it.expiration) : ''}</td>
      <td>${it ? escapeHtml(it.quantity) + ' ' + escapeHtml(it.unit) : ''}</td>
    </tr>`;
  }
  return `
  <div class="cl-form-page">
    <div class="cl-form-title-block">
      <div class="cl-agency">Charitable Pharmacy Destruction Log</div>
      <div class="cl-division">Non-Wisconsin-Drug-Repository-Program charitable donation source</div>
      <div class="cl-title">Source Program: ${escapeHtml(f.sourceProgram)}</div>
    </div>
    <ul class="cl-form-note">
      <li>This log documents destruction of medications received through a charitable donation program (e.g., Dispensary of Hope, Americares) that is separate from the Wisconsin Drug Repository Program.</li>
      <li>Retention requirements vary by program &mdash; e.g., Dispensary of Hope requires records be kept a minimum of 3 years. Confirm your specific program's requirement.</li>
      <li>Destruction must still comply with applicable state Board of Pharmacy regulations.</li>
    </ul>

    <div class="cl-section-header">Pharmacy or Medical Facility Information</div>
    <table class="cl-official">
      <tr>
        <td style="width:70%;"><span class="cl-label">Name &ndash; Pharmacy or Medical Facility</span><span class="cl-value">${escapeHtml(f.name)}</span></td>
        <td style="width:30%;"><span class="cl-label">Date Destroyed (MM/dd/yyyy)</span><span class="cl-value">${f.dateDestroyed}</span></td>
      </tr>
      <tr>
        <td><span class="cl-label">Street Address</span><span class="cl-value">${escapeHtml(f.address)}</span></td>
        <td><span class="cl-label">City / State / Zip</span><span class="cl-value">${escapeHtml(f.city)}, ${escapeHtml(f.state)} ${escapeHtml(f.zip)}</span></td>
      </tr>
      <tr>
        <td colspan="2"><span class="cl-label">Name &ndash; Person Destroying Drugs or Medical Supplies</span><span class="cl-value">${escapeHtml(f.person)}</span></td>
      </tr>
    </table>

    <div class="cl-section-header">Drug / Medical Supply Information</div>
    <table class="cl-official cl-drug-table">
      <thead>
        <tr>
          <th>Name of Drug or Medical Supply</th><th>Strength</th><th>NDC No.</th>
          <th>Lot No.</th><th>Expiration Date</th><th>Quantity Destroyed</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>

    ${isLastPage ? `
    <div class="cl-section-header">Signature</div>
    <table class="cl-official">
      <tr>
        <td style="width:30%;"><span class="cl-label">Date Signed (MM/dd/yyyy)</span><span class="cl-value">${f.dateSigned}</span></td>
        <td style="width:70%;">
          <span class="cl-label">Signature &ndash; Person Destroying Drugs or Medical Supplies</span>
          <div class="cl-sig-line"><img src="${sigDataUrl}" alt="signature"></div>
        </td>
      </tr>
    </table>` : `<p style="font-size:9px;color:#555;">(continued on next page)</p>`}
  </div>`;
}
