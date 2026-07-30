// Drug Repository Destruction Record — client-side only.
// GS1 DataMatrix parsing + GTIN->NDC candidate generation + openFDA lookup +
// camera/keyboard-wedge scanning live in the shared gs1-scanner.js (loaded
// before this file) — see that file for those details.

const state = {
  items: [], // {name, strength, ndc, lot, expiration, quantity}
};

const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) ||
  (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);

// ---------- Persistence for facility defaults (facility info only — no PHI, no signatures) ----------
const persistFields = ['facilityName', 'facilityAddress', 'facilityCity', 'facilityState', 'facilityZip'];
persistFields.forEach(id => {
  const el = document.getElementById(id);
  const saved = localStorage.getItem('destr_' + id);
  if (saved) el.value = saved;
  el.addEventListener('change', () => localStorage.setItem('destr_' + id, el.value));
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
initBarcodeScanner(async (text) => {
  const parsed = parseGS1(text);
  const gtin = parsed['01'] || '';
  const lot = parsed['10'] || '';
  const expiration = formatGS1Date(parsed['17'] || '');
  let name = '', strength = '', ndcDisplay = '', quantity = '';

  if (gtin) {
    // Fall back to the undashed raw 10 digits — honest about not knowing the
    // real labeler/product/package split — until/unless openFDA confirms one.
    ndcDisplay = gtin.length === 14 ? gtin.slice(3, 13) : '';
    const drug = await lookupDrugByGtin(gtin);
    if (drug) {
      name = drug.name; strength = drug.strength; ndcDisplay = drug.ndc;
      // Pre-filled from the package's declared unit count — still editable,
      // since this bottle may not have been full when it was destroyed.
      quantity = drug.quantity || '';
    } else {
      toast('Scanned OK, but no openFDA match — fill in name/strength manually');
    }
  } else {
    toast('Could not read GTIN from barcode — check item manually');
  }

  addItem({ name, strength, ndc: ndcDisplay, lot, expiration, quantity });
});

// ---------- Items table ----------
function addItem(item) {
  state.items.push(item);
  renderItems();
}

document.getElementById('addManualBtn').addEventListener('click', () => {
  addItem({ name: '', strength: '', ndc: '', lot: '', expiration: '', quantity: '' });
});

function renderItems() {
  const wrap = document.getElementById('itemsTableWrap');
  if (state.items.length === 0) {
    wrap.innerHTML = '<div class="dr-empty-state" id="emptyState">No items scanned yet</div>';
    return;
  }
  let html = '<div class="dr-table-scroll"><table><thead><tr>' +
    '<th class="dr-row-num">#</th><th>Name of Drug/Supply</th><th>Strength</th><th>NDC No.</th>' +
    '<th>Lot No.</th><th>Expiration</th><th title="Number of units (tablets/capsules/mL), not packages">Qty Destroyed (units)</th><th></th></tr></thead><tbody>';
  state.items.forEach((it, idx) => {
    html += `<tr>
      <td class="dr-row-num" data-label="#">${idx + 1}</td>
      <td data-label="Name"><input type="text" data-idx="${idx}" data-field="name" value="${escapeHtml(it.name)}"></td>
      <td data-label="Strength"><input type="text" data-idx="${idx}" data-field="strength" value="${escapeHtml(it.strength)}"></td>
      <td data-label="NDC"><input type="text" data-idx="${idx}" data-field="ndc" value="${escapeHtml(it.ndc)}"></td>
      <td data-label="Lot"><input type="text" data-idx="${idx}" data-field="lot" value="${escapeHtml(it.lot)}"></td>
      <td data-label="Exp"><input type="text" data-idx="${idx}" data-field="expiration" value="${escapeHtml(it.expiration)}"></td>
      <td data-label="Qty (units)"><input type="text" inputmode="numeric" placeholder="# units" title="Number of units (tablets/capsules/mL), not packages" class="${it.quantity ? '' : 'dr-qty-empty'}" data-idx="${idx}" data-field="quantity" value="${escapeHtml(it.quantity)}"></td>
      <td class="dr-remove-cell"><button class="dr-remove-btn" data-idx="${idx}" aria-label="Remove item">✕</button></td>
    </tr>`;
  });
  html += '</tbody></table></div>';
  wrap.innerHTML = html;

  wrap.querySelectorAll('input').forEach(inp => {
    inp.addEventListener('input', e => {
      const idx = +e.target.dataset.idx, field = e.target.dataset.field;
      state.items[idx][field] = e.target.value;
      if (field === 'quantity') {
        e.target.classList.toggle('dr-qty-empty', !e.target.value);
      }
    });
  });
  wrap.querySelectorAll('.dr-remove-btn').forEach(btn => {
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
function start(e) { drawing = true; hasSig = true; const p = pos(e); ctx.beginPath(); ctx.moveTo(p.x, p.y); document.getElementById('sigStatus').textContent = 'Signed'; document.getElementById('sigStatus').style.color = '#1a7a3a'; document.querySelector('.dr-sig-pad-wrap')?.classList.remove('field-missing'); e.preventDefault(); }
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
});

// ---------- Generate printable official record ----------
// Required header fields, checked top-to-bottom to match reading order —
// the first missing one gets scrolled to, highlighted, and named, rather
// than silently letting the record print with a blank field.
const REQUIRED_HEADER_FIELDS = [
  ['facilityName', 'the pharmacy/facility name'],
  ['facilityAddress', 'the street address'],
  ['facilityCity', 'the city'],
  ['facilityState', 'the state'],
  ['facilityZip', 'the zip code'],
  ['personDestroying', 'the name of the person destroying drugs/supplies'],
  ['dateDestroyed', 'the date destroyed'],
];

document.getElementById('generateBtn').addEventListener('click', () => {
  for (const [id, label] of REQUIRED_HEADER_FIELDS) {
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
  }
  if (!hasSig) {
    warnMissingField(document.querySelector('.dr-sig-pad-wrap'), 'Signature is required before generating the record');
    return;
  }
  const dateSignedEl = document.getElementById('dateSigned');
  if (!dateSignedEl.value) { warnMissingField(dateSignedEl, 'Enter the date signed before generating the record'); return; }

  const facility = {
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
      <td>${it ? escapeHtml(it.quantity) : ''}</td>
    </tr>`;
  }
  return `
  <div class="dr-form-page">
    <div class="dr-form-title-block">
      <span class="dr-formnum">F-62645C (05/2020)</span>
      <div class="dr-agency">DEPARTMENT OF HEALTH SERVICES STATE OF WISCONSIN</div>
      <div class="dr-division">Division of Quality Assurance</div>
      <div class="dr-title">Drug Repository Program &mdash; Destruction Record</div>
    </div>
    <ul class="dr-form-note">
      <li>Completion of this form meets the requirements of Wisconsin Administrative Code &sect; DHS 148.11(2) for destruction of drugs and medical supplies.</li>
      <li>Questions about completion of this form may be directed to 608-266-5388.</li>
    </ul>

    <div class="dr-section-header">Pharmacy or Medical Facility Information</div>
    <table class="dr-official">
      <tr>
        <td style="width:70%;"><span class="dr-label">Name &ndash; Pharmacy or Medical Facility</span><span class="dr-value">${escapeHtml(f.name)}</span></td>
        <td style="width:30%;"><span class="dr-label">Date Destroyed (MM/dd/yyyy)</span><span class="dr-value">${f.dateDestroyed}</span></td>
      </tr>
      <tr>
        <td><span class="dr-label">Street Address</span><span class="dr-value">${escapeHtml(f.address)}</span></td>
        <td><span class="dr-label">City / State / Zip</span><span class="dr-value">${escapeHtml(f.city)}, ${escapeHtml(f.state)} ${escapeHtml(f.zip)}</span></td>
      </tr>
      <tr>
        <td colspan="2"><span class="dr-label">Name &ndash; Person Destroying Drugs or Medical Supplies</span><span class="dr-value">${escapeHtml(f.person)}</span></td>
      </tr>
    </table>

    <div class="dr-section-header">Drug / Medical Supply Information</div>
    <table class="dr-official dr-drug-table">
      <thead>
        <tr>
          <th>Name of Drug or Medical Supply</th><th>Strength</th><th>NDC No.</th>
          <th>Lot No.</th><th>Expiration Date</th><th>Quantity Destroyed</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>

    ${isLastPage ? `
    <div class="dr-section-header">Signature</div>
    <table class="dr-official">
      <tr>
        <td style="width:30%;"><span class="dr-label">Date Signed (MM/dd/yyyy)</span><span class="dr-value">${f.dateSigned}</span></td>
        <td style="width:70%;">
          <span class="dr-label">Signature &ndash; Person Destroying Drugs or Medical Supplies</span>
          <div class="dr-sig-line"><img src="${sigDataUrl}" alt="signature"></div>
        </td>
      </tr>
    </table>` : `<p style="font-size:9px;color:#555;">(continued on next page)</p>`}
  </div>`;
}
