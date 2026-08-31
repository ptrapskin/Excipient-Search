// Drug Repository Transfer Record — client-side only. Mirrors the Drug
// Repository Donation Record tool but for the F-62645A transfer form
// (Wis. Admin. Code § DHS 148.09(3)), used when a pharmacy or medical
// facility distributes donated drugs/supplies to a participating repository.
//
// GS1 DataMatrix parsing + GTIN->NDC candidate generation + openFDA lookup +
// camera/keyboard-wedge scanning live in the shared gs1-scanner.js (loaded
// before this file).
//
// No patient/donor data here — transferring and receiving parties are both
// pharmacies/medical facilities, not patients — so none of the PHI-minimizing
// patterns from the Donation Record tool (idle auto-clear, no-persist donor
// fields) apply. Unlike the Donation Record, there is also NO 90-day
// expiration rule here: DHS 148.09(3) governs distribution between
// participants, not the original donation eligibility, so expiration is
// captured as plain data with no live warning or block.

const state = {
  items: [], // {name, strength, ndc, lot, expiration, quantity, unit}
};

const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) ||
  (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);

// ---------- Persistence for facility defaults ----------
// Both facilities are the user's own recurring counterparties, so all of
// their address fields persist as defaults (same approach as the Charitable
// Pharmacy Destruction Log). Dates and the transfer-scope choice are NOT
// persisted — they must be set explicitly for each record.
const persistFields = [
  'transName', 'transAddress', 'transCity', 'transState', 'transZip',
  'recvName', 'recvAddress', 'recvCity', 'recvState', 'recvZip',
  'repName',
];
persistFields.forEach(id => {
  const el = document.getElementById(id);
  const saved = localStorage.getItem('transfer_' + id);
  if (saved) el.value = saved;
  el.addEventListener('change', () => localStorage.setItem('transfer_' + id, el.value));
});

(function setDefaultDates() {
  const today = new Date().toISOString().slice(0, 10);
  document.getElementById('dateTransfer').value = today;
  document.getElementById('dateReceipt').value = today;
  document.getElementById('dateSigned').value = today;
})();

if (isIOS) {
  const hint = document.getElementById('iosPrintHint');
  if (hint) hint.style.display = 'block';
}

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

  addItem({ name, strength, ndc: ndcDisplay, lot, expiration, quantity, unit });
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

function renderItems() {
  const wrap = document.getElementById('itemsTableWrap');
  if (state.items.length === 0) {
    wrap.innerHTML = '<div class="dn-empty-state" id="emptyState">No items scanned yet</div>';
    return;
  }
  let html = '<div class="dn-table-scroll"><table><thead><tr>' +
    '<th class="dn-row-num">#</th><th>Name of Drug/Supply</th><th>Strength</th><th>NDC No.</th>' +
    '<th>Lot No.</th><th>Expiration</th><th title="Number of units, not packages">Qty Transferred (units)</th><th>Unit</th><th></th></tr></thead><tbody>';
  state.items.forEach((it, idx) => {
    html += `<tr>
      <td class="dn-row-num" data-label="#">${idx + 1}</td>
      <td data-label="Name"><input type="text" data-idx="${idx}" data-field="name" value="${escapeHtml(it.name)}"></td>
      <td data-label="Strength"><input type="text" data-idx="${idx}" data-field="strength" value="${escapeHtml(it.strength)}"></td>
      <td data-label="NDC"><input type="text" data-idx="${idx}" data-field="ndc" value="${escapeHtml(it.ndc)}"></td>
      <td data-label="Lot"><input type="text" data-idx="${idx}" data-field="lot" value="${escapeHtml(it.lot)}"></td>
      <td data-label="Exp"><input type="text" data-idx="${idx}" data-field="expiration" value="${escapeHtml(it.expiration)}"></td>
      <td data-label="Qty"><input type="text" inputmode="numeric" placeholder="e.g. 30" title="Number of units, not packages" data-idx="${idx}" data-field="quantity" value="${escapeHtml(it.quantity)}"></td>
      <td data-label="Unit">${unitSelectHtml(idx, it.unit)}</td>
      <td class="dn-remove-cell"><button class="dn-remove-btn" data-idx="${idx}" aria-label="Remove item">✕</button></td>
    </tr>`;
  });
  html += '</tbody></table></div>';
  wrap.innerHTML = html;

  wrap.querySelectorAll('input').forEach(inp => {
    inp.addEventListener('input', e => {
      const idx = +e.target.dataset.idx, field = e.target.dataset.field;
      state.items[idx][field] = e.target.value;
    });
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
function start(e) { drawing = true; hasSig = true; const p = pos(e); ctx.beginPath(); ctx.moveTo(p.x, p.y); document.getElementById('sigStatus').textContent = 'Signed'; document.getElementById('sigStatus').style.color = '#1a7a3a'; document.querySelector('.dn-sig-pad-wrap')?.classList.remove('field-missing'); e.preventDefault(); }
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
  if (!confirm('Clear all scanned items and the signature? Saved facility defaults are kept.')) return;
  preserveScroll(() => {
    document.querySelectorAll('.field-missing').forEach(el => el.classList.remove('field-missing'));
    state.items = [];
    renderItems();
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    hasSig = false;
    document.getElementById('sigStatus').textContent = 'Not signed';
    document.getElementById('sigStatus').style.color = '#c0392b';
    document.querySelectorAll('input[name="transferScope"]').forEach(r => { r.checked = false; });
  });
});

// ---------- Generate printable official record ----------
const REQUIRED_FIELDS = [
  ['transName', 'the transferring facility name'],
  ['transAddress', 'the transferring facility street address'],
  ['transCity', 'the transferring facility city'],
  ['transState', 'the transferring facility state'],
  ['transZip', 'the transferring facility zip code'],
  ['dateTransfer', 'the date of transfer'],
  ['recvName', 'the receiving facility name'],
  ['recvAddress', 'the receiving facility street address'],
  ['recvCity', 'the receiving facility city'],
  ['recvState', 'the receiving facility state'],
  ['recvZip', 'the receiving facility zip code'],
  ['dateReceipt', 'the date of receipt'],
];

document.getElementById('generateBtn').addEventListener('click', () => {
  for (const [id, label] of REQUIRED_FIELDS) {
    const el = document.getElementById(id);
    if (!el.value) { warnMissingField(el, `Enter ${label} before generating the record`); return; }
  }
  const scope = document.querySelector('input[name="transferScope"]:checked');
  if (!scope) {
    warnMissingField(document.querySelector('.dn-scope-card'), 'Choose whether the entire or a partial donation is being transferred');
    return;
  }
  if (state.items.length === 0) { toast('Add at least one item before generating the record'); return; }
  for (let i = 0; i < state.items.length; i++) {
    if (!state.items[i].quantity) {
      const el = document.querySelector(`#itemsTableWrap [data-field="quantity"][data-idx="${i}"]`);
      warnMissingField(el, `Enter a quantity for item ${i + 1} before generating the record`);
      return;
    }
    if (!state.items[i].unit) {
      const el = document.querySelector(`#itemsTableWrap [data-field="unit"][data-idx="${i}"]`);
      warnMissingField(el, `Pick a unit for item ${i + 1} before generating the record`);
      return;
    }
  }
  if (!hasSig) {
    warnMissingField(document.querySelector('.dn-sig-pad-wrap'), 'Signature is required before generating the record');
    return;
  }
  const repNameEl = document.getElementById('repName');
  if (!repNameEl.value) { warnMissingField(repNameEl, 'Enter the representative name before generating the record'); return; }
  const dateSignedEl = document.getElementById('dateSigned');
  if (!dateSignedEl.value) { warnMissingField(dateSignedEl, 'Enter the date signed before generating the record'); return; }

  preserveScroll(async () => {
    const f = {
      transName: document.getElementById('transName').value,
      transAddress: document.getElementById('transAddress').value,
      transCity: document.getElementById('transCity').value,
      transState: document.getElementById('transState').value,
      transZip: document.getElementById('transZip').value,
      recvName: document.getElementById('recvName').value,
      recvAddress: document.getElementById('recvAddress').value,
      recvCity: document.getElementById('recvCity').value,
      recvState: document.getElementById('recvState').value,
      recvZip: document.getElementById('recvZip').value,
      dateTransfer: fmtDate(document.getElementById('dateTransfer').value),
      dateReceipt: fmtDate(document.getElementById('dateReceipt').value),
      dateSigned: fmtDate(document.getElementById('dateSigned').value),
      repName: document.getElementById('repName').value,
      scope: document.querySelector('input[name="transferScope"]:checked').value,
    };
    const sigDataUrl = canvas.toDataURL('image/png');

    const pages = [];
    for (let i = 0; i < state.items.length; i += 8) pages.push(state.items.slice(i, i + 8));
    if (pages.length === 0) pages.push([]);

    let html = '';
    pages.forEach(pageItems => { html += buildFormPage(f, pageItems, sigDataUrl); });
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
  });
});

function fmtDate(iso) {
  if (!iso) return '';
  const [y, m, d] = iso.split('-');
  return `${m}/${d}/${y}`;
}

function buildFormPage(f, items, sigDataUrl) {
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
  const entireChecked = f.scope === 'entire' ? '&#9746;' : '&#9744;';
  const partialChecked = f.scope === 'partial' ? '&#9746;' : '&#9744;';
  return `
  <div class="dn-form-page">
    <div class="dn-form-title-block">
      <span class="dn-formnum">F-62645A (05/2020)</span>
      <div class="dn-agency">DEPARTMENT OF HEALTH SERVICES STATE OF WISCONSIN</div>
      <div class="dn-division">Division of Quality Assurance</div>
      <div class="dn-title">Drug Repository Program &mdash; Transfer Record</div>
    </div>
    <ul class="dn-form-note">
      <li>Completion of this form meets the requirements of Wisconsin Administrative Code &sect; DHS 148.09(3) for distribution of drugs or medical supplies to a participating repository.</li>
      <li>Questions about completion of this form may be directed to 608-266-5388.</li>
    </ul>

    <div class="dn-section-header">Transferring Pharmacy or Medical Facility Information</div>
    <table class="dn-official">
      <tr>
        <td style="width:70%;"><span class="dn-label">Name &ndash; Pharmacy or Medical Facility</span><span class="dn-value">${escapeHtml(f.transName)}</span></td>
        <td style="width:30%;"><span class="dn-label">Date of Transfer (MM/dd/yyyy)</span><span class="dn-value">${f.dateTransfer}</span></td>
      </tr>
      <tr>
        <td><span class="dn-label">Street Address</span><span class="dn-value">${escapeHtml(f.transAddress)}</span></td>
        <td><span class="dn-label">City / State / Zip</span><span class="dn-value">${escapeHtml(f.transCity)}, ${escapeHtml(f.transState)} ${escapeHtml(f.transZip)}</span></td>
      </tr>
    </table>

    <div class="dn-section-header">Receiving Pharmacy or Medical Facility Information</div>
    <table class="dn-official">
      <tr>
        <td style="width:70%;"><span class="dn-label">Name &ndash; Pharmacy or Medical Facility</span><span class="dn-value">${escapeHtml(f.recvName)}</span></td>
        <td style="width:30%;"><span class="dn-label">Date of Receipt (MM/dd/yyyy)</span><span class="dn-value">${f.dateReceipt}</span></td>
      </tr>
      <tr>
        <td><span class="dn-label">Street Address</span><span class="dn-value">${escapeHtml(f.recvAddress)}</span></td>
        <td><span class="dn-label">City / State / Zip</span><span class="dn-value">${escapeHtml(f.recvCity)}, ${escapeHtml(f.recvState)} ${escapeHtml(f.recvZip)}</span></td>
      </tr>
    </table>

    <div class="dn-section-header">Drug / Medical Supply Information</div>
    <table class="dn-official dn-drug-table">
      <thead>
        <tr>
          <th>Name of Drug or Medical Supply</th><th>Strength</th><th>NDC No.</th>
          <th>Lot No.</th><th>Expiration Date</th><th>Quantity Transferred</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>

    <div class="dn-section-header">Transferring Pharmacy or Medical Facility</div>
    <table class="dn-official">
      <tr><td style="font-size:13px;">${entireChecked} The entire original donation is being transferred. The original donation form is attached to this transfer record.</td></tr>
      <tr><td style="font-size:13px;">${partialChecked} The original donation is being partially transferred. A copy of the original donation form is attached to this transfer record.</td></tr>
    </table>

    <div class="dn-section-header">Attestation</div>
    <table class="dn-official">
      <tr><td style="font-size:13px;">I attest that the above-named drugs or medical supplies were stored as recommended by the manufacturer and that they have not been subject to tampering.</td></tr>
    </table>

    <div class="dn-section-header">Signature</div>
    <table class="dn-official">
      <tr>
        <td style="width:45%;">
          <span class="dn-label">Signature &ndash; Representative</span>
          <div class="dn-sig-line"><img src="${sigDataUrl}" alt="signature"></div>
        </td>
        <td style="width:35%;"><span class="dn-label">Name &ndash; Representative (Print or type.)</span><span class="dn-value">${escapeHtml(f.repName)}</span></td>
        <td style="width:20%;"><span class="dn-label">Date Signed (MM/dd/yyyy)</span><span class="dn-value">${f.dateSigned}</span></td>
      </tr>
    </table>
  </div>`;
}
