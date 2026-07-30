// Shared GS1 DataMatrix scanning + NDC lookup logic, used by both the Drug
// Destruction Record and Drug Donation Record tools. Load this script before
// either page's own script. No bundler in this app, so this is a plain
// classic script — its top-level function declarations are visible to any
// later <script> tag on the same page (no namespace object needed).

function toast(msg, ms = 2600) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  clearTimeout(t._timer);
  t._timer = setTimeout(() => t.classList.remove('show'), ms);
}

// Used by each tool's "Generate & Print" validation: a toast alone doesn't
// tell you WHICH field is empty, especially scrolled off-screen (a header
// field above, or an item row below a long list). This scrolls to it,
// highlights it, focuses it, and names it in the toast, then returns false
// so the caller can bail out of generation. Each page defines its own
// ".field-missing" CSS rule inside its scoped <style> block.
function warnMissingField(el, message) {
  if (el) {
    el.classList.add('field-missing');
    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    setTimeout(() => el.focus(), 300); // let the scroll settle before focusing
  }
  toast(message);
  return false;
}

// Clear a field's highlight the moment the user edits it, rather than making
// them re-click Generate just to find out it's fixed.
document.addEventListener('input', e => {
  if (e.target.classList) e.target.classList.remove('field-missing');
});

// Clicking Add/Reset/Generate re-renders the items table (changing page
// height) while also natively focusing the clicked button — the combination
// makes some browsers invoke their own "scroll the focused element into
// view" heuristic, silently jumping the page even though nothing asked it
// to. Wrap the action and forcibly re-assert the pre-click scroll position
// afterward, including a second pass after the wedge-scanner's delayed
// refocus (see initWedgeScanner) so that has the last word too.
function preserveScroll(fn) {
  const x = window.scrollX, y = window.scrollY;
  fn();
  requestAnimationFrame(() => window.scrollTo(x, y));
  setTimeout(() => window.scrollTo(x, y), 80);
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

// A confident guess at the unit a quantity should be counted in, from
// openFDA's dosage_form. Used by the donation tool's unit picker; left
// unused (harmless) by the destruction tool, which has no unit field.
function guessUnitFromDosageForm(dosageForm) {
  const f = (dosageForm || '').toUpperCase();
  if (f.includes('CAPSULE')) return 'capsules';
  if (f.includes('TABLET')) return 'tablets';
  if (f.includes('PATCH')) return 'patches';
  if (f.includes('SUPPOSITORY')) return 'suppositories';
  if (f.includes('VIAL') || f.includes('INJECT')) return 'vials';
  if (f.includes('SOLUTION') || f.includes('SUSPENSION') || f.includes('SYRUP') || f.includes('LIQUID') || f.includes('ELIXIR')) return 'mL';
  if (f.includes('POWDER') || f.includes('PACKET') || f.includes('GRANULE')) return 'packets';
  return ''; // no confident guess — leave for the user to pick
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
        return {
          ...formatDrugInfo(r),
          ndc: cand,
          quantity: quantityFromPackaging(r, raw10),
          unit: guessUnitFromDosageForm(r.dosage_form || ''),
        };
      }
    } catch (e) { /* try next candidate */ }
  }
  return null;
}

// A phone always uses the camera; a desktop/laptop is assumed to have a
// keyboard-wedge scanner instead. This doesn't need to be perfect — it just
// needs to not hide the camera button on an actual phone, which the
// pointer:coarse check alone reliably guarantees.
function isDesktopScannerMode() {
  const coarsePointer = window.matchMedia('(pointer: coarse)').matches;
  const narrowViewport = window.matchMedia('(max-width: 700px)').matches;
  return !coarsePointer && !narrowViewport;
}

// ---------- Camera + keyboard-wedge scanning ----------
// Wires up both input paths against a fixed set of element IDs that both
// pages share (startScanBtn, cancelScanBtn, scanner-wrap, scanStatus, video,
// cameraScanSection, wedgeStatus, wedgeCaptureInput). Calls onDecodedText(text)
// with the raw decoded payload from whichever path succeeds — the caller is
// responsible for parseGS1() + lookupDrugByGtin() + building its own item
// shape, since that differs slightly between the destruction and donation
// tools (donation also needs a unit field).
//
// iOS Safari notes:
//  - getUserMedia needs HTTPS + a direct tap (startScan is only ever called from
//    the button's click handler, never on page load).
//  - facingMode constraints are used instead of enumerateDevices() device-label
//    matching: iOS often reports generic/empty labels until a stream is already
//    granted, so label sniffing is unreliable there.
//  - the <video> element has playsinline + muted attributes in the template so
//    iOS renders it inline instead of forcing native fullscreen playback.
// zxing-wasm wraps the actively maintained ZXing-C++ core (WebAssembly) — the
// same decode engine native scanning apps use — and reads noticeably better
// on real, small/dense GS1 DataMatrix codes than the unmaintained pure-JS
// zxing-js. It doesn't ship a "watch this video element" helper, so we drive
// the camera ourselves: grab a frame to an offscreen canvas on a timer and
// hand the pixel data to ZXingWASM.readBarcodes().
function initBarcodeScanner(onDecodedText) {
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

  function handleDecodedText(text) {
    stopScan();
    toast('Barcode captured — looking up drug info…');
    onDecodedText(text);
  }

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
      handleDecodedText(results[0].text);
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

  initWedgeScanner();

  // ---------- Keyboard-wedge scanner (desktop) ----------
  // USB/Bluetooth 2D scanners in "keyboard wedge" mode need no drivers or
  // camera access — they just emit the decoded payload as keystrokes, followed
  // by Enter, exactly as if someone typed it very fast. We tell that apart from
  // real typing purely by speed: scanner keystrokes land well under
  // WEDGE_MAX_GAP_MS apart, sustained for the whole string; a human typing
  // can't sustain that pace. A gap longer than that resets the buffer, so
  // normal typing elsewhere on the page never accidentally gets treated as a
  // scan.
  function initWedgeScanner() {
    const WEDGE_MAX_GAP_MS = 50;
    const WEDGE_MIN_LENGTH = 8; // ignore stray/accidental short bursts
    // Most 2D scanners emit GS1's ASCII-29 "group separator" (the same GS
    // character parseGS1() already expects) between variable-length AI
    // fields, transmitted as a Ctrl+] keystroke since GS can't otherwise be
    // typed — that combination is detected automatically below. Some
    // scanners are configured to send a different printable delimiter
    // instead (or strip it entirely). If your scanner isn't producing a
    // parseable result, check its manual for an "AIM/GS1 field separator"
    // setting — many scanners are configured by scanning a special setup
    // barcode from the manual — and change WEDGE_DELIMITER below to
    // whatever character it actually transmits.
    const WEDGE_DELIMITER = GS;

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
    //
    // preventScroll:true is the primary defense — this refocus is invisible
    // housekeeping (so scanning can resume after clicking a button), not
    // something the user asked to jump to. It's not universally honored
    // though (older Safari ignores it entirely, pre-15.4), so back it with
    // an explicit scroll-position restore too: without either, every click
    // of Add/Reset/Generate would scroll the page to wherever this hidden
    // input sits.
    function focusWedgeInputInPlace() {
      const x = window.scrollX, y = window.scrollY;
      wedgeInput.focus({ preventScroll: true });
      if (window.scrollX !== x || window.scrollY !== y) window.scrollTo(x, y);
    }
    function refocusIfIdle() {
      const active = document.activeElement;
      const el = active && active.tagName;
      if (!active || active === document.body || el === 'BUTTON') focusWedgeInputInPlace();
    }
    focusWedgeInputInPlace();
    document.addEventListener('focusin', () => setTimeout(refocusIfIdle, 50));

    wedgeInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        const candidate = buffer;
        buffer = '';
        if (candidate.length >= WEDGE_MIN_LENGTH) {
          const normalized = WEDGE_DELIMITER === GS ? candidate : candidate.split(WEDGE_DELIMITER).join(GS);
          handleDecodedText(normalized);
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
}
