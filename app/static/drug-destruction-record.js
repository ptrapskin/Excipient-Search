// Drug Repository Destruction Record — client-side only.
// GS1 DataMatrix parsing + GTIN->NDC candidate generation + openFDA lookup are
// ported unchanged from the tested reference build. Everything camera/print
// related has been reworked for iOS Safari (see comments below).

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

function toast(msg, ms = 2600) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  clearTimeout(t._timer);
  t._timer = setTimeout(() => t.classList.remove('show'), ms);
}

// ---------- GS1 DataMatrix parsing ----------
const GS = String.fromCharCode(29); // FNC1 group separator as decoded by ZXing

function parseGS1(raw) {
  // Strip a leading FNC1 char some scanners emit
  let str = raw.replace(/^\]d2/i, '').replace(/^\)>/, '');
  const result = {};
  let i = 0;
  const fixedLen = { '00': 18, '01': 14, '11': 6, '13': 6, '15': 6, '17': 6 };
  while (i < str.length) {
    // AIs are 2, 3, or 4 digits; pharma barcodes use 2-digit AIs almost exclusively
    const ai = str.substr(i, 2);
    i += 2;
    if (fixedLen[ai] !== undefined) {
      result[ai] = str.substr(i, fixedLen[ai]);
      i += fixedLen[ai];
    } else {
      // variable length field, ends at GS separator or end of string
      let end = str.indexOf(GS, i);
      if (end === -1) end = str.length;
      result[ai] = str.substring(i, end);
      i = end;
      if (str[i] === GS) i += 1;
    }
  }
  return result;
}

function formatGS1Date(yymmdd) {
  if (!yymmdd || yymmdd.length !== 6) return '';
  const yy = yymmdd.slice(0, 2), mm = yymmdd.slice(2, 4), dd = yymmdd.slice(4, 6);
  const yyyy = (parseInt(yy, 10) < 50 ? '20' : '19') + yy;
  return `${mm}/${dd === '00' ? '01' : dd}/${yyyy}`;
}

// GTIN-14 -> candidate NDC formats. Digits 2-12 of the GTIN (11 digits) are the
// padded 5-4-2 NDC; de-pad each segment to try the 3 legal FDA formats too.
function buildCandidatesFrom11(ndc11) {
  if (!ndc11 || ndc11.length !== 11) return [];
  const A = ndc11.slice(0, 5), B = ndc11.slice(5, 9), C = ndc11.slice(9, 11);
  const candidates = new Set();
  candidates.add(`${A}-${B}`);              // as-is 5-4 (product_ndc)
  if (A[0] === '0') candidates.add(`${A.slice(1)}-${B}`);   // 4-4
  if (B[0] === '0') candidates.add(`${A}-${B.slice(1)}`);   // 5-3
  // full package_ndc variants (with package segment)
  candidates.add(`${A}-${B}-${C}`);
  if (A[0] === '0') candidates.add(`${A.slice(1)}-${B}-${C}`);
  if (B[0] === '0') candidates.add(`${A}-${B.slice(1)}-${C}`);
  if (C[0] === '0') candidates.add(`${A}-${B}-${C.slice(1)}`);
  return [...candidates];
}

async function lookupDrugByGtin(gtin14) {
  const candidates = buildCandidatesFrom11(gtin14.slice(1, 12));
  for (const cand of candidates) {
    try {
      const res = await fetch(`https://api.fda.gov/drug/ndc.json?search=product_ndc:"${cand}"+OR+package_ndc:"${cand}"&limit=1`);
      if (!res.ok) continue;
      const data = await res.json();
      if (data.results && data.results[0]) {
        const r = data.results[0];
        const name = r.brand_name || r.generic_name || '';
        let strength = '';
        if (r.active_ingredients && r.active_ingredients.length) {
          strength = r.active_ingredients.map(a => `${a.name} ${a.strength}`).join(', ');
        }
        return { name, strength };
      }
    } catch (e) { /* try next candidate */ }
  }
  return null;
}

// ---------- Scanner ----------
// iOS Safari notes:
//  - getUserMedia needs HTTPS + a direct tap (startScan is only ever called from
//    the button's click handler, never on page load).
//  - facingMode constraints are used instead of enumerateDevices() device-label
//    matching: iOS often reports generic/empty labels until a stream is already
//    granted, so label sniffing is unreliable there.
//  - the <video> element has playsinline + muted attributes in the template so
//    iOS renders it inline instead of forcing native fullscreen playback.
let codeReader = null;
let scanTimeoutHandle = null;
const SCAN_TIMEOUT_MS = 90000; // auto-stop if left scanning idle, to save battery/heat
const startBtn = document.getElementById('startScanBtn');
const cancelBtn = document.getElementById('cancelScanBtn');
const scannerWrap = document.getElementById('scanner-wrap');
const scanStatus = document.getElementById('scanStatus');

startBtn.addEventListener('click', startScan);
cancelBtn.addEventListener('click', () => stopScan());

document.addEventListener('visibilitychange', () => {
  if (document.hidden) stopScan();
});
window.addEventListener('pagehide', () => stopScan());

async function startScan() {
  scannerWrap.classList.add('active');
  startBtn.style.display = 'none';
  scanStatus.textContent = 'Point camera at the GS1 DataMatrix barcode';
  const { BrowserMultiFormatReader, DecodeHintType, BarcodeFormat } = ZXing;
  const hints = new Map();
  hints.set(DecodeHintType.POSSIBLE_FORMATS, [BarcodeFormat.DATA_MATRIX]);
  // Throttle the decode loop (default is as-fast-as-possible) so scanning
  // doesn't pin the CPU/heat the phone if a user leaves the camera open.
  codeReader = new BrowserMultiFormatReader(hints, 250);

  clearTimeout(scanTimeoutHandle);
  scanTimeoutHandle = setTimeout(() => {
    stopScan();
    toast('Scanning stopped after 90 seconds of inactivity — tap Scan Item to try again');
  }, SCAN_TIMEOUT_MS);

  try {
    const constraints = { video: { facingMode: { ideal: 'environment' } } };
    await codeReader.decodeFromConstraints(constraints, 'video', (result, err) => {
      if (result) onScanSuccess(result.getText());
    });
  } catch (e) {
    let msg = 'Camera error: ' + e.message;
    if (e.name === 'NotAllowedError') {
      msg = 'Camera access was denied. Enable camera access for this site in Settings and try again.';
    } else if (e.name === 'NotFoundError') {
      msg = 'No camera was found on this device.';
    }
    scanStatus.textContent = msg;
  }
}

function stopScan() {
  clearTimeout(scanTimeoutHandle);
  if (codeReader) { codeReader.reset(); codeReader = null; }
  scannerWrap.classList.remove('active');
  startBtn.style.display = 'block';
}

async function onScanSuccess(text) {
  stopScan();
  toast('Barcode captured — looking up drug info…');
  const parsed = parseGS1(text);
  const gtin = parsed['01'] || '';
  const lot = parsed['10'] || '';
  const expiration = formatGS1Date(parsed['17'] || '');
  let name = '', strength = '', ndcDisplay = '';

  if (gtin) {
    ndcDisplay = buildCandidatesFrom11(gtin.slice(1, 12))[0] || '';
    const drug = await lookupDrugByGtin(gtin);
    if (drug) { name = drug.name; strength = drug.strength; }
    else toast('Scanned OK, but no openFDA match — fill in name/strength manually');
  } else {
    toast('Could not read GTIN from barcode — check item manually');
  }

  addItem({ name, strength, ndc: ndcDisplay, lot, expiration, quantity: '' });
}

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
    '<th>Lot No.</th><th>Expiration</th><th>Qty Destroyed</th><th></th></tr></thead><tbody>';
  state.items.forEach((it, idx) => {
    html += `<tr>
      <td class="dr-row-num">${idx + 1}</td>
      <td><input type="text" data-idx="${idx}" data-field="name" value="${escapeHtml(it.name)}"></td>
      <td><input type="text" data-idx="${idx}" data-field="strength" value="${escapeHtml(it.strength)}"></td>
      <td><input type="text" data-idx="${idx}" data-field="ndc" value="${escapeHtml(it.ndc)}"></td>
      <td><input type="text" data-idx="${idx}" data-field="lot" value="${escapeHtml(it.lot)}"></td>
      <td><input type="text" data-idx="${idx}" data-field="expiration" value="${escapeHtml(it.expiration)}"></td>
      <td><input type="text" inputmode="numeric" class="${it.quantity ? '' : 'dr-qty-empty'}" data-idx="${idx}" data-field="quantity" value="${escapeHtml(it.quantity)}"></td>
      <td><button class="dr-remove-btn" data-idx="${idx}" aria-label="Remove item">✕</button></td>
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

// ---------- Generate printable official record ----------
document.getElementById('generateBtn').addEventListener('click', () => {
  if (state.items.length === 0) { toast('Add at least one item before generating the record'); return; }
  const missingQty = state.items.some(it => !it.quantity);
  if (missingQty) { toast('Enter a quantity for every item before generating'); return; }
  if (!hasSig) { toast('Signature is required before generating the record'); return; }

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
