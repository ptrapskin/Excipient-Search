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
const persistFields = ['recipientName'];
persistFields.forEach(id => {
  const el = document.getElementById(id);
  const saved = localStorage.getItem('donation_' + id);
  if (saved) el.value = saved;
  el.addEventListener('change', () => localStorage.setItem('donation_' + id, el.value));
});

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
  preserveScroll(() => addItem({ name: '', strength: '', ndc: '', lot: '', expiration: '', quantity: '', unit: '' }));
});

function unitSelectHtml(idx, selected) {
  const opts = UNIT_OPTIONS.map(u =>
    `<option value="${u}" ${u === selected ? 'selected' : ''}>${u}</option>`
  ).join('');
  return `<select class="dn-unit-select ${selected ? '' : 'dn-qty-empty'}" data-idx="${idx}" data-field="unit">
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
    '<th>Lot No.</th><th>Expiration</th><th title="Number of units, not packages">Qty Donated (units)</th><th>Unit</th><th></th></tr></thead><tbody>';
  state.items.forEach((it, idx) => {
    html += `<tr>
      <td class="dn-row-num" data-label="#">${idx + 1}</td>
      <td data-label="Name"><input type="text" data-idx="${idx}" data-field="name" value="${escapeHtml(it.name)}"></td>
      <td data-label="Strength"><input type="text" data-idx="${idx}" data-field="strength" value="${escapeHtml(it.strength)}"></td>
      <td data-label="NDC"><input type="text" data-idx="${idx}" data-field="ndc" value="${escapeHtml(it.ndc)}"></td>
      <td data-label="Lot"><input type="text" data-idx="${idx}" data-field="lot" value="${escapeHtml(it.lot)}"></td>
      <td data-label="Exp"><input type="text" data-idx="${idx}" data-field="expiration" value="${escapeHtml(it.expiration)}"></td>
      <td data-label="Qty"><input type="text" inputmode="numeric" placeholder="e.g. 30" title="Number of units, not packages" class="${it.quantity ? '' : 'dn-qty-empty'}" data-idx="${idx}" data-field="quantity" value="${escapeHtml(it.quantity)}"></td>
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
      if (field === 'quantity') {
        e.target.classList.toggle('dn-qty-empty', !e.target.value);
      }
    });
  });
  wrap.querySelectorAll('select.dn-unit-select').forEach(sel => {
    sel.addEventListener('change', e => {
      const idx = +e.target.dataset.idx;
      state.items[idx].unit = e.target.value;
      e.target.classList.toggle('dn-qty-empty', !e.target.value);
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
  if (!confirm('Clear all scanned items and the signature? Recipient info is kept, donor info is cleared.')) return;
  preserveScroll(() => {
    // Clear any red "missing field" highlight left over from a prior failed
    // Generate attempt — without this, Reset could look like it did nothing
    // if the only visible symptom was a highlighted header field.
    document.querySelectorAll('.field-missing').forEach(el => el.classList.remove('field-missing'));
    state.items = [];
    renderItems();
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    hasSig = false;
    document.getElementById('sigStatus').textContent = 'Not signed';
    document.getElementById('sigStatus').style.color = '#c0392b';
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
  ['recipientName', 'the recipient facility name'],
];

document.getElementById('generateBtn').addEventListener('click', () => {
  for (const [id, label] of REQUIRED_FIELDS) {
    const el = document.getElementById(id);
    if (!el.value) { warnMissingField(el, `Enter ${label} before generating the record`); return; }
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
  const dateSignedEl = document.getElementById('dateSigned');
  if (!dateSignedEl.value) { warnMissingField(dateSignedEl, 'Enter the date signed before generating the record'); return; }

  preserveScroll(async () => {
    const donor = {
      name: document.getElementById('donorName').value,
      address: document.getElementById('donorAddress').value,
      city: document.getElementById('donorCity').value,
      state: document.getElementById('donorState').value,
      zip: document.getElementById('donorZip').value,
      dateDonated: fmtDate(document.getElementById('dateDonated').value),
      dateSigned: fmtDate(document.getElementById('dateSigned').value),
      recipientName: document.getElementById('recipientName').value,
    };
    const sigDataUrl = canvas.toDataURL('image/png');

    const pages = [];
    for (let i = 0; i < state.items.length; i += 10) pages.push(state.items.slice(i, i + 10));

    let html = '';
    pages.forEach((pageItems, pIdx) => {
      html += buildFormPage(donor, pageItems, sigDataUrl, pIdx === pages.length - 1);
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

    ${isLastPage ? `
    <div class="dn-section-header">Attestation</div>
    <table class="dn-official">
      <tr><td style="font-size:9.5px;">I attest that the above-named drugs or medical supplies were stored as recommended by the manufacturer and have not been subject to tampering.</td></tr>
    </table>

    <div class="dn-section-header">Signature</div>
    <table class="dn-official">
      <tr>
        <td style="width:30%;"><span class="dn-label">Date Signed (MM/dd/yyyy)</span><span class="dn-value">${f.dateSigned}</span></td>
        <td style="width:70%;">
          <span class="dn-label">Signature &ndash; Donor</span>
          <div class="dn-sig-line"><img src="${sigDataUrl}" alt="signature"></div>
        </td>
      </tr>
    </table>` : `<p style="font-size:9px;color:#555;">(continued on next page)</p>`}
  </div>`;
}
