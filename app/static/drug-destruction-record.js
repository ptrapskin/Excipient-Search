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

// GTIN-14 structure for NDC-based GS1 healthcare barcodes (verified against
// real, independently-documented GTIN/NDC pairs — e.g. GTIN 00349281547584 is
// ActHIB NDC 49281-547-58; GTIN 00300051971015 is Prevnar 13 NDC 0005-1971-01):
//   digit 1        = GS1 indicator digit
//   digits 2-3     = constant "03" flag (marks "an NDC follows"), NOT part of the NDC
//   digits 4-13    = the 10-digit NDC exactly as printed on the label, digits only
//   digit 14       = check digit
// The 10 raw digits split into labeler/product/package using one of 3 legal
// FDA formats (4-4-2, 5-3-2, 5-4-1) — the barcode doesn't say which, so we
// build a candidate for each and let the openFDA lookup find the real match.
function buildNdcCandidatesFromGtin(gtin14) {
  if (!gtin14 || gtin14.length !== 14) return [];
  const raw10 = gtin14.slice(3, 13);
  if (raw10.length !== 10) return [];
  const splits = [
    [raw10.slice(0, 4), raw10.slice(4, 8), raw10.slice(8, 10)],  // 4-4-2
    [raw10.slice(0, 5), raw10.slice(5, 8), raw10.slice(8, 10)],  // 5-3-2
    [raw10.slice(0, 5), raw10.slice(5, 9), raw10.slice(9, 10)],  // 5-4-1
  ];
  const candidates = new Set();
  for (const [labeler, product, pkg] of splits) {
    candidates.add(`${labeler}-${product}`);        // product_ndc form
    candidates.add(`${labeler}-${product}-${pkg}`);  // package_ndc form
  }
  return [...candidates];
}

function titleCase(s) {
  return (s || '').toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

// openFDA returns ALL-CAPS brand/generic names and per-ingredient strength
// strings like "30 mg/1" (i.e. "per 1 unit"). Compose these the way a
// pharmacist actually writes them: "Brand (generic)" and "30 mg tablet"
// rather than repeating the ingredient name in the strength field.
function formatDrugInfo(r) {
  const brand = r.brand_name ? titleCase(r.brand_name) : '';
  const generic = r.generic_name ? r.generic_name.toLowerCase() : '';
  let name = brand || titleCase(generic);
  if (brand && generic && generic !== brand.toLowerCase()) {
    name = `${brand} (${generic})`;
  }

  let strength = '';
  if (r.active_ingredients && r.active_ingredients.length) {
    const perIngredient = r.active_ingredients
      .map(a => (a.strength || '').split('/')[0].trim())
      .filter(Boolean);
    strength = perIngredient.join('/');
    if (r.dosage_form) strength = `${strength} ${r.dosage_form.toLowerCase()}`.trim();
  }
  return { name, strength };
}

// Find the exact package the scanned barcode refers to (matched by raw digit
// content, not by which format guess happened to succeed) and read its
// declared unit count off openFDA's "N TABLET in 1 BOTTLE"-style description.
// Multi-level packaging (e.g. "2 BOTTLE in 1 CARTON / 14 TABLET in 1 BOTTLE")
// would need multiplying across levels to get a true total — rather than
// guess wrong, we skip auto-fill for those and leave it for manual entry.
function quantityFromPackaging(r, raw10) {
  if (!r.packaging) return '';
  const pkg = r.packaging.find(p => (p.package_ndc || '').replace(/-/g, '') === raw10);
  if (!pkg || !pkg.description || pkg.description.includes('/')) return '';
  const match = /^(\d+(?:\.\d+)?)\s/.exec(pkg.description);
  return match ? match[1] : '';
}

async function lookupDrugByGtin(gtin14) {
  const raw10 = gtin14.length === 14 ? gtin14.slice(3, 13) : '';
  const candidates = buildNdcCandidatesFromGtin(gtin14);
  for (const cand of candidates) {
    try {
      // package_ndc is nested under "packaging" in openFDA's schema — a bare
      // package_ndc:"..." clause matches nothing, so this must query
      // packaging.package_ndc instead.
      const res = await fetch(`https://api.fda.gov/drug/ndc.json?search=product_ndc:"${cand}"+OR+packaging.package_ndc:"${cand}"&limit=1`);
      if (!res.ok) continue;
      const data = await res.json();
      if (data.results && data.results[0]) {
        const r = data.results[0];
        return { ...formatDrugInfo(r), ndc: cand, quantity: quantityFromPackaging(r, raw10) };
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
// zxing-js (the old decode engine here) is an unmaintained pure-JS port with
// known-weak DataMatrix reliability. zxing-wasm wraps the actively maintained
// ZXing-C++ core (WebAssembly) — the same decode engine native scanning apps
// use — and reads noticeably better on real, small/dense GS1 DataMatrix
// codes. It doesn't ship a "watch this video element" helper like zxing-js
// did, so we drive the camera ourselves: grab a frame to an offscreen canvas
// on a timer and hand the pixel data to ZXingWASM.readBarcodes().
let mediaStream = null;
let decodeLoopHandle = null;
let decodeBusy = false;
let scanTimeoutHandle = null;
const SCAN_TIMEOUT_MS = 90000; // auto-stop if left scanning idle, to save battery/heat
const DECODE_INTERVAL_MS = 300;
const startBtn = document.getElementById('startScanBtn');
const cancelBtn = document.getElementById('cancelScanBtn');
const scannerWrap = document.getElementById('scanner-wrap');
const scanStatus = document.getElementById('scanStatus');
const videoEl = document.getElementById('video');
const scanCanvas = document.createElement('canvas');
const scanCtx = scanCanvas.getContext('2d', { willReadFrequently: true });

startBtn.addEventListener('click', startScan);
cancelBtn.addEventListener('click', () => stopScan());

document.addEventListener('visibilitychange', () => {
  if (document.hidden) stopScan();
});
window.addEventListener('pagehide', () => stopScan());

async function startScan() {
  scannerWrap.classList.add('active');
  startBtn.style.display = 'none';
  scanStatus.textContent = 'Hold the phone a few inches away so the barcode fills most of the frame';

  // Kick off the ~1.5MB WASM module fetch/instantiate in parallel with the
  // camera permission prompt so the first frame decode isn't stalled on it.
  ZXingWASM.prepareZXingModule({ fireImmediately: true }).catch(() => {});

  clearTimeout(scanTimeoutHandle);
  scanTimeoutHandle = setTimeout(() => {
    stopScan();
    toast('Scanning stopped after 90 seconds of inactivity — tap Scan Item to try again');
  }, SCAN_TIMEOUT_MS);

  try {
    // Request the highest resolution the device offers — GS1 DataMatrix
    // modules are physically tiny on pharma packaging, so decode success
    // is very sensitive to how many camera pixels actually land on the code.
    const constraints = {
      video: {
        facingMode: { ideal: 'environment' },
        width: { ideal: 1920 },
        height: { ideal: 1080 },
      },
    };
    mediaStream = await navigator.mediaDevices.getUserMedia(constraints);
    videoEl.srcObject = mediaStream;
    await videoEl.play();
    if (!videoEl.videoWidth) {
      await new Promise(resolve => videoEl.addEventListener('loadedmetadata', resolve, { once: true }));
    }
    scheduleDecode();
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

function scheduleDecode() {
  decodeLoopHandle = setTimeout(async () => {
    if (decodeBusy || !mediaStream) return;
    decodeBusy = true;
    try {
      await decodeOneFrame();
    } catch (e) {
      // Ignore individual frame failures (e.g. WASM module still warming up
      // on the very first tick) — the loop just tries again next tick.
    } finally {
      decodeBusy = false;
      if (mediaStream) scheduleDecode();
    }
  }, DECODE_INTERVAL_MS);
}

async function decodeOneFrame() {
  if (!videoEl.videoWidth) return;
  scanCanvas.width = videoEl.videoWidth;
  scanCanvas.height = videoEl.videoHeight;
  scanCtx.drawImage(videoEl, 0, 0, scanCanvas.width, scanCanvas.height);
  const imageData = scanCtx.getImageData(0, 0, scanCanvas.width, scanCanvas.height);
  const results = await ZXingWASM.readBarcodes(imageData, {
    formats: ['DataMatrix'],
    tryHarder: true,
    textMode: 'Plain', // raw payload text, matching what parseGS1() expects
  });
  if (results && results.length) {
    onScanSuccess(results[0].text);
  }
}

function stopScan() {
  clearTimeout(scanTimeoutHandle);
  clearTimeout(decodeLoopHandle);
  if (mediaStream) {
    mediaStream.getTracks().forEach(track => track.stop());
    mediaStream = null;
  }
  videoEl.srcObject = null;
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
}

// ---------- Keyboard-wedge scanner (desktop) ----------
// USB/Bluetooth 2D scanners in "keyboard wedge" mode need no drivers or
// camera access — they just emit the decoded payload as keystrokes, followed
// by Enter, exactly as if someone typed it very fast. We tell that apart from
// real typing purely by speed: scanner keystrokes land well under
// WEDGE_MAX_GAP_MS apart, sustained for the whole string; a human typing
// can't sustain that pace. A gap longer than that resets the buffer, so
// normal typing elsewhere on the page never accidentally gets treated as a
// scan.
const WEDGE_MAX_GAP_MS = 50;
const WEDGE_MIN_LENGTH = 8; // ignore stray/accidental short bursts

// Most 2D scanners emit GS1's ASCII-29 "group separator" (the same GS
// character parseGS1() already expects) between variable-length AI fields,
// transmitted as a Ctrl+] keystroke since GS can't otherwise be typed — that
// combination is detected automatically below. Some scanners are configured
// to send a different printable delimiter instead (or strip it entirely).
// If your scanner isn't producing a parseable result, check its manual for
// an "AIM/GS1 field separator" setting — many scanners are configured by
// scanning a special setup barcode from the manual — and change
// WEDGE_DELIMITER below to whatever character it actually transmits.
let WEDGE_DELIMITER = GS;

// A phone always uses the camera; a desktop/laptop is assumed to have a
// keyboard-wedge scanner instead. This doesn't need to be perfect — it just
// needs to not hide the camera button on an actual phone, which the
// pointer:coarse check alone reliably guarantees.
function isDesktopScannerMode() {
  const coarsePointer = window.matchMedia('(pointer: coarse)').matches;
  const narrowViewport = window.matchMedia('(max-width: 700px)').matches;
  return !coarsePointer && !narrowViewport;
}

function initWedgeScanner() {
  const wedgeInput = document.getElementById('wedgeCaptureInput');
  const wedgeStatus = document.getElementById('wedgeStatus');
  const cameraSection = document.getElementById('cameraScanSection');
  if (!wedgeInput || !wedgeStatus || !cameraSection) return;

  const desktop = isDesktopScannerMode();
  wedgeStatus.style.display = desktop ? 'flex' : 'none';
  cameraSection.style.display = desktop ? 'none' : 'block';
  if (!desktop) return;

  let buffer = '';
  let lastKeyTime = 0;

  // Keep the hidden capture input focused so wedge keystrokes always land
  // here, except while the user is legitimately typing into a real field —
  // in that case leave focus alone rather than stealing it mid-edit.
  function refocusIfIdle() {
    const active = document.activeElement;
    const el = active && active.tagName;
    if (!active || active === document.body || el === 'BUTTON') wedgeInput.focus();
  }
  wedgeInput.focus();
  document.addEventListener('focusin', () => setTimeout(refocusIfIdle, 50));

  wedgeInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      const candidate = buffer;
      buffer = '';
      if (candidate.length >= WEDGE_MIN_LENGTH) {
        const normalized = WEDGE_DELIMITER === GS ? candidate : candidate.split(WEDGE_DELIMITER).join(GS);
        onScanSuccess(normalized);
      }
      return;
    }

    const isGsCombo = e.ctrlKey && e.key === ']';
    const isPrintable = !isGsCombo && e.key.length === 1 && !e.ctrlKey && !e.metaKey && !e.altKey;
    if (!isGsCombo && !isPrintable) return; // ignore Shift, Tab, arrows, etc.
    e.preventDefault();

    const now = performance.now();
    if (buffer && now - lastKeyTime > WEDGE_MAX_GAP_MS) {
      buffer = ''; // gap too long to be a scanner burst — start over
    }
    buffer += isGsCombo ? GS : e.key;
    lastKeyTime = now;
  });
}

initWedgeScanner();

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
