'use strict';

const COUNTRIES = {AT:'Австрия',BE:'Бельгия',BG:'Болгария',HR:'Хорватия',CY:'Кипр',CZ:'Чехия',DK:'Дания',EE:'Эстония',FI:'Финляндия',FR:'Франция',DE:'Германия',GR:'Греция',HU:'Венгрия',IE:'Ирландия',IT:'Италия',LV:'Латвия',LT:'Литва',LU:'Люксембург',MT:'Мальта',NL:'Нидерланды',PT:'Португалия',RO:'Румыния',SK:'Словакия',SI:'Словения',ES:'Испания',SE:'Швеция'};
const PAGE = 25;
const $ = id => document.getElementById(id);
const fmt = n => n.toLocaleString('ru-RU');
const plural = (n, one, few, many) => { const a = n % 10, b = n % 100; return a === 1 && b !== 11 ? one : a >= 2 && a <= 4 && (b < 12 || b > 14) ? few : many; };
const norm = s => String(s).trim().toUpperCase().replace(/\s+/g, '');

/* ---------- IndexedDB ---------- */
let dbp;
function db() {
  return dbp ??= new Promise((res, rej) => {
    const r = indexedDB.open('zipfinder', 1);
    r.onupgradeneeded = () => { r.result.createObjectStore('data', {keyPath: 'cc'}); r.result.createObjectStore('meta', {keyPath: 'cc'}); };
    r.onsuccess = () => res(r.result);
    r.onerror = () => rej(r.error);
  });
}
async function tx(stores, mode, fn) {
  const d = await db();
  return new Promise((res, rej) => {
    const t = d.transaction(stores, mode);
    let out;
    Promise.resolve(fn(t)).then(v => out = v);
    t.oncomplete = () => res(out);
    t.onerror = () => rej(t.error);
    t.onabort = () => rej(t.error || new Error('Транзакция отменена'));
  });
}
const req = r => new Promise((res, rej) => { r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); });

let meta = {};               // cc -> {cc, n, at}
const cache = new Map();     // cc -> {zip, nz, place, lat, lon}

async function loadMeta() {
  const rows = await tx(['meta'], 'readonly', t => req(t.objectStore('meta').getAll()));
  meta = Object.fromEntries(rows.map(r => [r.cc, r]));
}
async function getData(cc) {
  if (cache.has(cc)) return cache.get(cc);
  const row = await tx(['data'], 'readonly', t => req(t.objectStore('data').get(cc)));
  if (!row) return null;
  row.nz = row.zip.map(norm);
  cache.set(cc, row);
  return row;
}
async function saveCountry(cc, b) {
  const row = {cc, zip: b.zip, place: b.place, lat: Float64Array.from(b.lat), lon: Float64Array.from(b.lon)};
  const m = {cc, n: b.zip.length, at: Date.now()};
  await tx(['data', 'meta'], 'readwrite', t => { t.objectStore('data').put(row); t.objectStore('meta').put(m); });
  meta[cc] = m; cache.delete(cc);
}
async function deleteCountries(list) {
  await tx(['data', 'meta'], 'readwrite', t => { for (const cc of list) { t.objectStore('data').delete(cc); t.objectStore('meta').delete(cc); } });
  for (const cc of list) { delete meta[cc]; cache.delete(cc); }
}

/* ---------- Импорт GeoNames (.txt и .zip потоком, без распаковки вручную) ---------- */
function counter(onBytes) {
  let n = 0;
  return new TransformStream({transform(chunk, c) { n += chunk.byteLength; onBytes(n); c.enqueue(chunk); }});
}
async function parseLines(byteStream, builders) {
  const reader = byteStream.pipeThrough(new TextDecoderStream()).getReader();
  let buf = '', added = 0;
  const take = line => {
    if (!line) return;
    const v = line.split('\t');
    if (v.length < 11) return;
    const cc = v[0].toUpperCase();
    if (!COUNTRIES[cc]) return;
    const zip = v[1].trim(), la = Number(v[9]), lo = Number(v[10]);
    if (!zip || !Number.isFinite(la) || !Number.isFinite(lo)) return;
    const b = builders[cc] ??= {zip: [], place: [], lat: [], lon: []};
    b.zip.push(zip); b.place.push(v[2] || ''); b.lat.push(la); b.lon.push(lo);
    added++;
  };
  for (;;) {
    const {value, done} = await reader.read();
    if (done) break;
    buf += value;
    const parts = buf.split(/\r?\n/);
    buf = parts.pop();
    for (const p of parts) take(p);
  }
  take(buf.replace(/\r$/, ''));
  return added;
}
async function zipEntries(file) {
  const tailLen = Math.min(file.size, 65557);
  const tail = new DataView(await file.slice(file.size - tailLen).arrayBuffer());
  let e = -1;
  for (let i = tail.byteLength - 22; i >= 0; i--) if (tail.getUint32(i, true) === 0x06054b50) { e = i; break; }
  if (e < 0) throw new Error('файл не похож на ZIP-архив');
  const cdSize = tail.getUint32(e + 12, true), cdOff = tail.getUint32(e + 16, true);
  if (cdOff === 0xFFFFFFFF) throw new Error('архив ZIP64 не поддерживается, распакуй его в «Файлах»');
  const cd = new DataView(await file.slice(cdOff, cdOff + cdSize).arrayBuffer());
  const out = [], dec = new TextDecoder();
  for (let p = 0; p + 46 <= cd.byteLength && cd.getUint32(p, true) === 0x02014b50;) {
    const method = cd.getUint16(p + 10, true), csize = cd.getUint32(p + 20, true);
    const nlen = cd.getUint16(p + 28, true), elen = cd.getUint16(p + 30, true), clen = cd.getUint16(p + 32, true);
    const lho = cd.getUint32(p + 42, true);
    const name = dec.decode(new Uint8Array(cd.buffer, cd.byteOffset + p + 46, nlen));
    out.push({name, method, csize, lho});
    p += 46 + nlen + elen + clen;
  }
  return out;
}
async function zipStream(file, ent) {
  const lh = new DataView(await file.slice(ent.lho, ent.lho + 30).arrayBuffer());
  if (lh.getUint32(0, true) !== 0x04034b50) throw new Error('повреждён архив');
  const start = ent.lho + 30 + lh.getUint16(26, true) + lh.getUint16(28, true);
  const raw = file.slice(start, start + ent.csize).stream();
  return {raw, method: ent.method};
}

async function importFiles(files) {
  const builders = {};
  const total = files.reduce((s, f) => s + f.size, 0);
  let doneBytes = 0, added = 0;
  showProgress(0, 'Читаю файлы…');
  for (const f of files) {
    const onBytes = n => showProgress((doneBytes + n) / total, `${f.name}: ${Math.round(((doneBytes + n) / total) * 100)}%`);
    if (/\.zip$/i.test(f.name) || f.type === 'application/zip') {
      const ents = (await zipEntries(f)).filter(x => /\.txt$/i.test(x.name) && !/readme/i.test(x.name));
      if (!ents.length) throw new Error(`в ${f.name} нет файла .txt с индексами`);
      for (const ent of ents) {
        const {raw, method} = await zipStream(f, ent);
        let s = raw.pipeThrough(counter(onBytes));
        if (method === 8) {
          if (typeof DecompressionStream === 'undefined') throw new Error('этот браузер не умеет распаковывать ZIP, распакуй архив в «Файлах» и выбери .txt');
          s = s.pipeThrough(new DecompressionStream('deflate-raw'));
        } else if (method !== 0) throw new Error(`неизвестный метод сжатия в ${f.name}`);
        added += await parseLines(s, builders);
      }
    } else {
      added += await parseLines(f.stream().pipeThrough(counter(onBytes)), builders);
    }
    doneBytes += f.size;
  }
  const ccs = Object.keys(builders);
  showProgress(1, `Сохраняю ${ccs.length} стран в память телефона…`);
  for (const cc of ccs) await saveCountry(cc, builders[cc]);
  try { await navigator.storage?.persist?.(); } catch {}
  return {added, ccs};
}

/* ---------- Поиск ---------- */
function distance(a1, o1, a2, o2) {
  const r = Math.PI / 180, dl = (a2 - a1) * r, dn = (o2 - o1) * r;
  const x = Math.sin(dl / 2) ** 2 + Math.cos(a1 * r) * Math.cos(a2 * r) * Math.sin(dn / 2) ** 2;
  return 12742 * Math.asin(Math.min(1, Math.sqrt(x)));
}
function findNear(d, z, radius) {
  const base = [];
  for (let i = 0; i < d.nz.length; i++) if (d.nz[i] === z) base.push(i);
  if (!base.length) return null;
  const best = new Map();
  for (const b of base) {
    const la = d.lat[b], lo = d.lon[b];
    const dLat = radius / 110.5, dLon = radius / (111.3 * Math.max(0.2, Math.cos(la * Math.PI / 180)));
    for (let i = 0; i < d.nz.length; i++) {
      const k = d.nz[i];
      if (k === z || Math.abs(d.lat[i] - la) > dLat || Math.abs(d.lon[i] - lo) > dLon) continue;
      const km = distance(la, lo, d.lat[i], d.lon[i]);
      if (km > radius) continue;
      const old = best.get(k);
      if (!old || km < old.km) best.set(k, {zip: d.zip[i], place: d.place[i], km});
    }
  }
  return {origin: d.place[base[0]], list: [...best.values()].sort((a, b) => a.km - b.km)};
}

/* ---------- Состояние сторон A/B ---------- */
const state = {side: 'A', A: {cc: '', zip: '', radius: 30, res: null, shown: PAGE}, B: {cc: '', zip: '', radius: 30, res: null, shown: PAGE}};
function saveState() {
  try { localStorage.setItem('zf-state', JSON.stringify({side: state.side, A: pick(state.A), B: pick(state.B)})); } catch {}
}
const pick = s => ({cc: s.cc, zip: s.zip, radius: s.radius});
function restoreState() {
  try {
    const s = JSON.parse(localStorage.getItem('zf-state') || 'null');
    if (!s) return;
    state.side = s.side === 'B' ? 'B' : 'A';
    Object.assign(state.A, s.A); Object.assign(state.B, s.B);
  } catch {}
}
const cur = () => state[state.side];

/* ---------- UI ---------- */
const el = (tag, cls, text) => { const x = document.createElement(tag); if (cls) x.className = cls; if (text != null) x.textContent = text; return x; };

function renderCountrySelect() {
  const sel = $('country'), have = Object.keys(meta).sort((a, b) => COUNTRIES[a].localeCompare(COUNTRIES[b], 'ru'));
  const rest = Object.keys(COUNTRIES).filter(c => !meta[c]).sort((a, b) => COUNTRIES[a].localeCompare(COUNTRIES[b], 'ru'));
  sel.replaceChildren(new Option('Выбери страну', ''));
  const group = (label, list) => {
    if (!list.length) return;
    const g = document.createElement('optgroup'); g.label = label;
    for (const c of list) g.append(new Option(`${COUNTRIES[c]} · ${c}`, c));
    sel.append(g);
  };
  group('В базе', have); group('Нет в базе', rest);
  sel.value = cur().cc || '';
}
function renderBaseChip() {
  const ccs = Object.keys(meta), n = ccs.reduce((s, c) => s + meta[c].n, 0);
  $('openBase').classList.toggle('ready', ccs.length > 0);
  $('baseLabel').textContent = ccs.length ? `База: ${ccs.length === 1 ? ccs[0] : ccs.length + ' стр.'}` : 'База не загружена';
  $('openBase').setAttribute('aria-label', ccs.length ? `База индексов: ${ccs.length} стран, ${fmt(n)} записей. Открыть` : 'База индексов не загружена. Открыть');
  const list = $('countryList');
  list.replaceChildren();
  for (const c of ccs.sort()) {
    const li = el('li'), l = el('span');
    l.append(el('span', 'cc', c), el('span', '', COUNTRIES[c] + ' '), el('span', 'n', fmt(meta[c].n)));
    const del = el('button', 'del', 'Удалить'); del.type = 'button';
    del.setAttribute('aria-label', `Удалить ${COUNTRIES[c]} из базы`);
    del.onclick = async () => { await deleteCountries([c]); refreshAll(); toast(`${COUNTRIES[c]} удалена из базы`); };
    li.append(l, del); list.append(li);
  }
  $('clearAll').hidden = ccs.length < 2;
}
function renderLegs() {
  for (const s of ['A', 'B']) {
    const st = state[s], tab = $('tab' + s), on = state.side === s;
    tab.setAttribute('aria-selected', on); tab.tabIndex = on ? 0 : -1;
    $('sub' + s).classList.toggle('mono', !!(st.cc && st.zip));
    $('sub' + s).textContent = st.cc && st.zip ? `${st.cc} ${st.zip}` : 'не задано';
  }
  $('panel').setAttribute('aria-labelledby', 'tab' + state.side);
}
function syncForm() {
  const s = cur();
  $('country').value = s.cc || '';
  $('zip').value = s.zip || '';
  const r = document.querySelector(`#radius input[value="${s.radius}"]`) || document.querySelector('#radius input[value="30"]');
  r.checked = true;
  $('zip').placeholder = state.side === 'A' ? '10115' : '75001';
  for (const id of ['country', 'zip']) $(id).removeAttribute('aria-invalid');
}

function stateBox(title, text, opts = {}) {
  const box = el('div', 'empty' + (opts.error ? ' error' : ''));
  box.append(el('h2', '', title));
  if (text) box.append(el('p', '', text));
  if (opts.action) {
    const b = el('button', 'primary', opts.action.label); b.type = 'button'; b.onclick = opts.action.fn;
    const p = el('p'); p.append(b); box.append(p);
  }
  return box;
}
function renderResults() {
  const out = $('results'), s = cur();
  out.replaceChildren();
  if (!Object.keys(meta).length) {
    out.append(stateBox('Сначала загрузи базу индексов',
      'Один раз выбери архивы стран GeoNames. Дальше поиск работает без интернета.',
      {action: {label: 'Открыть базу', fn: openBase}}));
    return;
  }
  if (s.msg) { out.append(s.msg()); return; }
  if (!s.res) return;
  const {list, origin} = s.res;
  const head = el('div', 'res-head'), p = el('p');
  p.append(el('b', '', fmt(list.length)), ` ${plural(list.length, 'индекс', 'индекса', 'индексов')} в ${s.res.radius} км от ${s.res.cc} ${s.res.zip}${origin ? ' (' + origin + ')' : ''}`);
  head.append(p);
  if (list.length) {
    const all = el('button', 'ghost', 'Копировать все'); all.type = 'button';
    all.onclick = () => copy(list.map(x => x.zip).join(', '), `Скопировано: ${list.length} ${plural(list.length, 'индекс', 'индекса', 'индексов')}`);
    head.append(all);
  }
  out.append(head);
  if (!list.length) { out.append(stateBox('Рядом нет других индексов', 'Попробуй радиус побольше.')); return; }
  const ul = el('ul', 'list');
  for (const x of list.slice(0, s.shown)) {
    const li = el('li', 'item'), info = el('div'), meta2 = el('div', 'meta');
    meta2.append(el('span', 'km', x.km.toFixed(1) + ' км'), ' · ' + (x.place || 'без названия'));
    info.append(el('div', 'zip', x.zip), meta2);
    const b = el('button', 'copy', 'Копировать'); b.type = 'button';
    b.setAttribute('aria-label', `Копировать индекс ${x.zip}`);
    b.onclick = () => copy(x.zip, `${x.zip} скопирован`, b);
    li.append(info, b); ul.append(li);
  }
  out.append(ul);
  if (list.length > s.shown) {
    const more = el('button', 'more', `Показать ещё ${Math.min(PAGE, list.length - s.shown)} из ${list.length - s.shown}`); more.type = 'button';
    more.onclick = () => { s.shown += PAGE; renderResults(); };
    out.append(more);
  }
}

async function runSearch() {
  const s = cur();
  s.cc = $('country').value; s.zip = $('zip').value.trim();
  s.radius = Number(document.querySelector('#radius input:checked').value);
  saveState();
  $('country').setAttribute('aria-invalid', !s.cc);
  $('zip').setAttribute('aria-invalid', !s.zip);
  s.res = null; s.shown = PAGE;
  if (!s.cc || !s.zip) {
    s.msg = () => stateBox('Не хватает данных', !s.cc ? 'Выбери страну.' : 'Введи исходный индекс.', {error: true});
    renderLegs(); renderResults(); (!s.cc ? $('country') : $('zip')).focus(); return;
  }
  if (!meta[s.cc]) {
    s.msg = () => stateBox(`Страны «${COUNTRIES[s.cc]}» нет в базе`, `Загрузи архив ${s.cc}.zip с GeoNames.`, {action: {label: 'Открыть базу', fn: openBase}});
    renderLegs(); renderResults(); return;
  }
  s.msg = () => stateBox('Ищу…', null);
  renderResults();
  $('go').disabled = true;
  try {
    const d = await getData(s.cc);
    if (!d) throw new Error('данные страны не найдены в памяти, загрузи её заново');
    const z = norm(s.zip), r = findNear(d, z, s.radius);
    if (!r) {
      s.msg = () => stateBox(`Индекс ${s.zip} не найден`, `Страна: ${COUNTRIES[s.cc]}. Проверь написание. Пример формата: ${d.zip[Math.floor(d.zip.length / 2)]}.`, {error: true});
    } else {
      s.msg = null;
      s.res = {...r, cc: s.cc, zip: s.zip, radius: s.radius};
    }
  } catch (err) {
    s.msg = () => stateBox('Не получилось выполнить поиск', String(err.message || err), {error: true, action: {label: 'Повторить', fn: runSearch}});
  } finally {
    $('go').disabled = false;
  }
  renderLegs(); renderResults();
}

/* ---------- Копирование и тост ---------- */
let toastT;
function toast(text) {
  const t = $('toast'); t.textContent = text; t.classList.add('show');
  clearTimeout(toastT); toastT = setTimeout(() => t.classList.remove('show'), 1600);
}
async function copy(text, msg, btn) {
  let ok = false;
  try { await navigator.clipboard.writeText(text); ok = true; } catch {
    const t = el('textarea'); t.value = text; t.setAttribute('readonly', ''); t.style.position = 'fixed'; t.style.opacity = '0';
    document.body.append(t); t.select(); t.setSelectionRange(0, text.length);
    try { ok = document.execCommand('copy'); } catch {}
    t.remove();
  }
  toast(ok ? msg : 'Не удалось скопировать');
  if (btn && ok) { btn.textContent = 'Готово'; btn.classList.add('done'); setTimeout(() => { btn.textContent = 'Копировать'; btn.classList.remove('done'); }, 1400); }
}

/* ---------- Лист «База» ---------- */
function openBase() { const d = $('baseDialog'); if (!d.open) d.showModal(); }
function showProgress(frac, text) {
  $('progress').hidden = false;
  $('barFill').style.width = Math.round(frac * 100) + '%';
  $('progressText').textContent = text;
}
function refreshAll() { renderBaseChip(); renderCountrySelect(); renderLegs(); renderResults(); }

function bind() {
  $('openBase').onclick = openBase;
  $('closeBase').onclick = () => $('baseDialog').close();
  $('baseDialog').addEventListener('click', e => { if (e.target === e.currentTarget) e.currentTarget.close(); });
  $('files').addEventListener('change', async e => {
    const files = [...e.target.files];
    e.target.value = '';
    if (!files.length) return;
    const msg = $('baseMsg'); msg.textContent = ''; msg.classList.remove('err');
    $('files').disabled = true;
    try {
      const {added, ccs} = await importFiles(files);
      $('progress').hidden = true;
      msg.textContent = ccs.length ? `Готово: ${fmt(added)} ${plural(added, 'запись', 'записи', 'записей')}, страны: ${ccs.join(', ')}.` : 'В файлах нет индексов стран ЕС (Польша пропускается).';
      if (ccs.length === 1 && !cur().cc) { cur().cc = ccs[0]; saveState(); }
      refreshAll();
    } catch (err) {
      $('progress').hidden = true;
      msg.classList.add('err');
      msg.textContent = 'Ошибка: ' + (err.message || err) + '. Выбери файл ещё раз.';
    } finally {
      $('files').disabled = false;
    }
  });
  $('clearAll').onclick = async () => {
    if (!confirm('Удалить все страны из базы? Их придётся загрузить заново.')) return;
    await deleteCountries(Object.keys(meta)); refreshAll(); toast('База очищена');
  };

  const tabs = [$('tabA'), $('tabB')];
  const select = side => {
    if (state.side === side) return;
    const s = cur(); s.cc = $('country').value; s.zip = $('zip').value.trim();
    state.side = side; saveState(); syncForm(); renderLegs(); renderResults();
  };
  tabs.forEach(t => {
    t.onclick = () => select(t.dataset.side);
    t.onkeydown = e => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(e.key)) return;
      e.preventDefault();
      const next = state.side === 'A' ? 'B' : 'A';
      select(next); $('tab' + next).focus();
    };
  });
  $('panel').addEventListener('submit', e => { e.preventDefault(); $('zip').blur(); runSearch(); });
  $('radius').addEventListener('change', () => { if (cur().res) runSearch(); else { cur().radius = Number(document.querySelector('#radius input:checked').value); saveState(); } });
  $('country').addEventListener('change', () => { cur().cc = $('country').value; $('country').removeAttribute('aria-invalid'); saveState(); });
  $('zip').addEventListener('input', () => $('zip').removeAttribute('aria-invalid'));
}

async function init() {
  bind();
  restoreState();
  try { await loadMeta(); } catch (err) {
    $('results').replaceChildren(stateBox('Память браузера недоступна', 'Без неё базу нельзя сохранить. Выключи приватный режим Safari и открой приложение снова.', {error: true}));
    return;
  }
  renderCountrySelect(); syncForm(); refreshAll();
  if ('serviceWorker' in navigator && location.protocol !== 'file:') navigator.serviceWorker.register('sw.js').catch(() => {});
}
init();
