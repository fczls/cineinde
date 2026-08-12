/**
 * L'ÉCRAN D'ARRIVÉE — « la fenêtre »
 * ═══════════════════════════════════════════════════════════════════════════
 * Annonce la semaine programmée, fait défiler ses nouveautés dans une fenêtre
 * fixe, puis s'efface en emportant l'écran ENTIER dans le pli WebGL déjà
 * utilisé par l'éventail des évènements (src/cloth.js). Le pli révèle le site
 * réel, qui s'est chargé derrière pendant la séquence — c'est le seul intérêt
 * défendable d'un écran qui retient : il occupe le temps de chargement au lieu
 * de s'y ajouter.
 *
 * Prototype et banc d'essai : design/lab/arrivee.html (les autres partis pris
 * y restent ouvrables ; seule « la fenêtre » a été retenue).
 *
 * Le module est chargé en import DYNAMIQUE depuis index.html et ne s'exécute
 * que si l'amorce (dans src/template.html) a jugé que l'écran devait jouer.
 * ═══════════════════════════════════════════════════════════════════════════
 */
import { creerCloth, CLOTH_DEFAUTS, bezier } from './cloth.js';

// ═══════════════════════════════════════════════════════════════════════════
// RÉGLAGES
// ═══════════════════════════════════════════════════════════════════════════
// Trois affiches, pas une de plus : la séquence s'ouvre DIRECTEMENT sur la
// première (aucun défilement d'amorce) et le pli de sortie part pendant que la
// troisième est encore à l'écran — une quatrième n'apparaît jamais.
const AFFICHES_MAX = 3;
const PAS   = 400;    // durée de la transition d'une affiche à la suivante
const POSE  = 1200;   // temps où l'affiche reste immobile à l'écran
const INTERIMAGE = 16;         // px — la barre noire entre deux photogrammes
const SEUIL_MOBILE = 520;      // px — doit rester aligné sur le @media de components.css

// Filet de sécurité : au-delà de ce délai sans données, l'écran s'efface. Une
// intro qui attend le réseau n'est plus une intro, c'est une panne.
const ATTENTE_MAX = 2600;

const AFFICHE_REPLI = 'assets/visual-fallback.webp';
const FOND = 'assets/texture-abstraite-claire-1.webp';
const LOGO = 'assets/logo.svg';

// Préréglage du pli de sortie, issu du labo (design/lab/cloth.html → « Copier »).
// `rayon` à 0 : la sortie emporte l'écran ENTIER, qui est rectangulaire — un
// arrondi y rognerait les quatre coins de la page.
const PRESET_SORTIE = {
  duree: 2400, retard: 0, ampliRebond: 0,
  angle: -90, nbPlis: 1.5, amplitude: 0.3, arete: 0,
  desordre: 0.1, derive: 0.2, nbRides: 2.4, amplitudeRides: 0.26,
  retrait: 1, balayage: 1.2, traineBord: 0.55,
  chute: 0.05, approche: 0.06, profondeurZ: 0,
  perspective: 0.32, rotation: 5, debordement: 1.28,
  lumiereX: -0.45, lumiereY: 0.7, ombre: 0.3, eclat: 0.16, translucide: 0.22,
  iriIntensite: 0.42, iriFresnel: 2.6, iriEchelle: 1.35,
  iriDecalage: 0.1, iriVitesse: 0.25, iriRepos: 0.03,
  grain: 0.03, voile: 0.9, opacite: 1, subdivisions: 64, rayon: 0,
};
const COURBE_SORTIE = [0.291, 0.2388, 0.2864, 0.9742];

// ═══════════════════════════════════════════════════════════════════════════
// DONNÉES — « nouveauté » = première séance JAMAIS enregistrée par le système,
// tombant dans la semaine courante (mercredi → mardi).
//
// ⚠️ Le chargeur du site ne peut pas servir ce calcul : il ne demande que
// `date >= today` (invariant I8) et ne voit donc pas ce qui a commencé avant
// aujourd'hui. On pagine ici la table `seances` en entier, sur deux colonnes —
// ~1 500 lignes, 2 requêtes, ~300 ms mesurés. Viable aujourd'hui, mais ça
// grossit : le jour où ça pèse, une fonction SQL `nouveautes()` rend le même
// résultat en un appel et sans plafond.
//
// Le coût réseau est masqué : la requête part en parallèle du chargement du
// site, et l'écran dure de toute façon plus longtemps qu'elle.
// ═══════════════════════════════════════════════════════════════════════════
const iso = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;

function bornesSemaine(ref = new Date()) {
  const d = new Date(ref); d.setHours(0, 0, 0, 0);
  const dow = d.getDay();                       // 0 = dimanche, 3 = mercredi
  const debut = new Date(d); debut.setDate(d.getDate() - (dow >= 3 ? dow - 3 : dow + 4));
  const fin = new Date(debut); fin.setDate(debut.getDate() + 6);
  return { debut, fin };
}

const normaliser = t => (t || '').toLowerCase()
  .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
  .replace(/[^a-z0-9]+/g, ' ').trim();

// Une affiche n'est texturable que si son origine sert `Access-Control-Allow-Origin`.
// Les affiches rapatriées dans Supabase Storage et celles de TMDB le font ;
// `cinemas-lumiere.com` non — d'où la substitution dans la capture.
const texturable = url => /supabase\.co\/storage|image\.tmdb\.org/.test(url || '');

async function chargerNouveautes({ client, url, cle }) {
  const jour = iso(new Date());
  const cache = sessionStorage.getItem('intro-nouveautes');
  if (cache) {
    const c = JSON.parse(cache);
    if (c.jour === jour) return c.data;
  }

  const sb = client.createClient(url, cle);

  // Toute la table, par pages de 1 000 (plafond REST). Deux colonnes seulement.
  let seances = [], from = 0;
  for (;;) {
    const { data, error } = await sb.from('seances')
      .select('film_id,date,heure').order('date').range(from, from + 999);
    if (error) throw error;
    seances = seances.concat(data);
    if (data.length < 1000) break;
    from += 1000;
  }
  const { data: films, error: errFilms } = await sb.from('films')
    .select('id,titre,poster').limit(1000);
  if (errFilms) throw errFilms;

  // Première séance jamais vue, par film.
  const premiere = {};
  for (const s of seances) {
    const p = premiere[s.film_id];
    if (!p || s.date < p.date || (s.date === p.date && s.heure < p.heure)) {
      premiere[s.film_id] = { date: s.date, heure: s.heure };
    }
  }

  const { debut, fin } = bornesSemaine();
  const d0 = iso(debut), d1 = iso(fin);

  const vus = new Set();
  const nouveautes = films
    .map(f => ({ ...f, premiere: premiere[f.id] }))
    .filter(f => f.premiere && f.premiere.date >= d0 && f.premiere.date <= d1)
    // Doublons de la table `films` (variantes de casse). Le site a `dedupeFilms`,
    // l'intro se contente de la clé de titre normalisé : elle ne montre que des
    // affiches, deux lignes fusionnées n'y changent rien d'autre que le compte.
    .filter(f => { const k = normaliser(f.titre); if (vus.has(k)) return false; vus.add(k); return true; })
    .sort((a, b) => (a.premiere.date + a.premiere.heure).localeCompare(b.premiere.date + b.premiere.heure))
    .map(f => ({ titre: f.titre, poster: f.poster }));

  const data = { debut: d0, fin: d1, nouveautes };
  try { sessionStorage.setItem('intro-nouveautes', JSON.stringify({ jour, data })); } catch (_) {}
  return data;
}

// ═══════════════════════════════════════════════════════════════════════════
// SORTIE EN TISSU — plier l'écran d'arrivée TOUT ENTIER
// ═══════════════════════════════════════════════════════════════════════════
// `cloth.charger()` ne prend qu'une URL : pour plier l'écran entier il faut
// d'abord le rasteriser. Pas de html2canvas (30 Ko pour un seul appel), pas de
// <foreignObject> (il ne sérialise pas les images distantes, or l'écran n'est
// QUE des images). On recompose donc la scène à la main sur un contexte 2D.
// C'est une approximation assumée : le pli écrase le détail dès la première image.
//
// ⚠️ Deux pièges de teinture. (1) Une image affichée sans `crossOrigin` teinte
// le canvas MÊME si le serveur envoie les en-têtes CORS — c'est le mode de la
// requête qui compte, pas la réponse ; on recharge donc chaque affiche via une
// seconde Image en mode anonyme (le cache la sert, c'est gratuit). (2) Une
// affiche d'origine non partageuse ne peut pas l'être : on lui substitue le
// visuel local, seulement dans la capture — l'écran, lui, garde la vraie.

/** object-fit:cover sur un contexte 2D. */
function couvrir(g, img, x, y, w, h, px = .5, py = .5) {
  const ri = img.naturalWidth / img.naturalHeight, rb = w / h;
  let sw = img.naturalWidth, sh = img.naturalHeight;
  if (ri > rb) sw = sh * rb; else sh = sw / rb;
  g.drawImage(img, (img.naturalWidth - sw) * px, (img.naturalHeight - sh) * py, sw, sh, x, y, w, h);
}

const chargerCORS = src => new Promise(res => {
  const im = new Image();
  im.crossOrigin = 'anonymous';
  im.onload = () => res(im);
  im.onerror = () => res(null);
  im.src = src;
});

async function capturerIntro(ecran) {
  const W = ecran.clientWidth, H = ecran.clientHeight;
  if (!W || !H) return null;
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const cv = document.createElement('canvas');
  cv.width = Math.round(W * dpr); cv.height = Math.round(H * dpr);
  const g = cv.getContext('2d');
  g.scale(dpr, dpr);
  // Le fond de la capture est LU sur l'écran : le token peut bouger, la
  // rasterisation suivra sans qu'on ait à la reteindre à la main.
  g.fillStyle = getComputedStyle(ecran).backgroundColor || '#000';
  g.fillRect(0, 0, W, H);

  const base = ecran.getBoundingClientRect();
  const boite = el => {
    const r = el.getBoundingClientRect();
    return { x: r.left - base.left, y: r.top - base.top, w: r.width, h: r.height };
  };

  // ── Le fond ──────────────────────────────────────────────────────────────
  // Dessiné depuis l'élément LUI-MÊME, sans rechargement : le visuel est servi
  // par notre propre origine, et une image de même origine ne teint jamais le
  // canvas. Le détour par `chargerCORS` lui coûterait une seconde requête —
  // le mode anonyme est une autre clé de cache, il ne retomberait pas dessus.
  const fond = ecran.querySelector('.fn-fond img');
  if (fond && fond.naturalWidth) {
    // La boîte est MESURÉE sur le rendu plutôt que recalculée : la capture suit
    // le CSS sans le réinterpréter.
    const b = boite(fond);
    couvrir(g, fond, b.x, b.y, b.w, b.h, .5, .5);
  }

  // ── Les affiches ─────────────────────────────────────────────────────────
  const photos = [...ecran.querySelectorAll('.fn-photo')];
  const sources = photos.map(ph => {
    const im = ph.querySelector('img');
    const src = im.currentSrc || im.src;
    return texturable(src) ? src : AFFICHE_REPLI;   // origine muette → visuel local
  });
  const images = await Promise.all(sources.map(chargerCORS));
  const rayon = parseFloat(getComputedStyle(photos[0] || ecran).borderTopLeftRadius) || 0;

  photos.forEach((ph, k) => {
    const im = images[k]; if (!im) return;
    const b = boite(ph);
    if (b.x > W || b.x + b.w < 0 || b.y > H || b.y + b.h < 0) return;   // hors cadre
    g.save();
    g.beginPath();
    if (g.roundRect) g.roundRect(b.x, b.y, b.w, b.h, rayon); else g.rect(b.x, b.y, b.w, b.h);
    g.clip();
    couvrir(g, im, b.x, b.y, b.w, b.h);
    g.restore();
  });

  // ── Le voile mobile, par-dessus l'affiche et sous le texte ───────────────
  const voile = ecran.querySelector('.fn-voile');
  if (voile && getComputedStyle(voile).opacity !== '0') {
    g.fillStyle = getComputedStyle(voile).backgroundColor;
    g.fillRect(0, 0, W, H);
  }

  // ── Le logo et les textes ────────────────────────────────────────────────
  const logo = ecran.querySelector('.fn-logo');
  if (logo && logo.naturalWidth) {          // même origine : pas de rechargement
    const b = boite(logo);
    g.drawImage(logo, b.x, b.y, b.w, b.h);
  }

  // ⚠️ Un nœud de texte à la fois, positionné par un Range — pas
  // `el.textContent`. Ces libellés mélangent deux styles (« SEMAINE » tranche
  // sur la date, le compte sur le reste) : lire le texte d'un bloc recolle les
  // fragments sans leur espacement (« AU MARDI 11 AOÛT13 SORTIES ») et perd les
  // couleurs. Le Range donne la position ET le style réels de chaque bout.
  const noeudsTexte = racine => {
    const out = [];
    (function parcourir(n) {
      if (n.nodeType === 3) { if (n.textContent.trim()) out.push(n); return; }
      n.childNodes.forEach(parcourir);
    })(racine);
    return out;
  };

  // Un texte peint par `background-clip:text` a une couleur de remplissage
  // TRANSPARENTE : recopiée telle quelle, la ligne s'effacerait au moment du
  // pli. On rejoue donc le dégradé sur le canvas, en le RELISANT sur l'élément
  // — `background-image` est déjà résolu par le navigateur (les `var()` y sont
  // évaluées), il n'y a pas de seconde écriture des couleurs qui divergerait.
  const degradeTexte = (el, b) => {
    const cs = getComputedStyle(el);
    if (cs.webkitTextFillColor && !/rgba\(0, 0, 0, 0\)/.test(cs.webkitTextFillColor)) return null;
    const fond = cs.backgroundImage;
    if (!fond || fond === 'none') return null;
    const angle = (parseFloat((fond.match(/(-?[\d.]+)deg/) || [])[1]) || 180) * Math.PI / 180;
    const arrets = [...fond.matchAll(/(rgba?\([^)]+\))\s*([\d.]+)%/g)];
    if (arrets.length < 2) return null;
    // Repère CSS : 0deg pointe vers le HAUT, l'angle tourne dans le sens des
    // aiguilles. La longueur de la ligne est la projection de la boîte dessus.
    const dx = Math.sin(angle), dy = -Math.cos(angle);
    const L = Math.abs(b.w * dx) + Math.abs(b.h * dy);
    const cx = b.x + b.w / 2, cy = b.y + b.h / 2;
    const grad = g.createLinearGradient(cx - dx * L / 2, cy - dy * L / 2,
                                        cx + dx * L / 2, cy + dy * L / 2);
    arrets.forEach(([, couleur, pos]) => grad.addColorStop(
      Math.min(1, Math.max(0, parseFloat(pos) / 100)), couleur));
    return grad;
  };

  for (const el of ecran.querySelectorAll('.fn-ligne, .fn-nb')) {
    const degrade = degradeTexte(el, boite(el));
    for (const noeud of noeudsTexte(el)) {
      const cs = getComputedStyle(noeud.parentElement);
      const rg = document.createRange(); rg.selectNodeContents(noeud);
      const r = rg.getBoundingClientRect();
      if (!r.width) continue;
      g.font = `${cs.fontWeight} ${cs.fontSize} ${cs.fontFamily}`;
      if ('letterSpacing' in g) g.letterSpacing = cs.letterSpacing;
      g.fillStyle = degrade || cs.color;
      g.textAlign = 'left';
      g.textBaseline = 'middle';
      const texte = cs.textTransform === 'uppercase'
        ? noeud.textContent.toUpperCase() : noeud.textContent;
      // Pas de `trim()` : le Range mesure l'espace de tête, le retirer collerait
      // le fragment au précédent (« 13SORTIES »).
      g.fillText(texte, r.left - base.left, r.top - base.top + r.height / 2);
    }
  }

  // ── Le filet ─────────────────────────────────────────────────────────────
  const filet = ecran.querySelector('.fn-filet');
  if (filet) {
    const b = boite(filet), rempli = boite(filet.querySelector('i'));
    const cs = getComputedStyle(filet);
    g.fillStyle = cs.backgroundColor;
    g.fillRect(b.x, b.y, b.w, Math.max(1, b.h));
    g.fillStyle = getComputedStyle(filet.querySelector('i')).backgroundColor;
    g.fillRect(b.x, b.y, rempli.w, Math.max(1, b.h));
  }

  try { return cv.toDataURL('image/jpeg', 0.92); }
  catch (e) { console.warn('[CinémasLyon] capture d\'intro teintée — sortie en fondu', e); return null; }
}

/**
 * Plie l'écran d'arrivée et l'emporte, découvrant le site. Retombe sur le
 * fondu CSS si le WebGL manque ou si la capture échoue — la sortie ne doit
 * JAMAIS rester bloquée.
 */
async function sortirEnTissu(ecran, webglOk) {
  if (!webglOk) return false;
  const W = ecran.clientWidth, H = ecran.clientHeight;
  const url = await capturerIntro(ecran);
  if (!url) return false;

  const p = { ...CLOTH_DEFAUTS, ...PRESET_SORTIE };
  const canvas = document.createElement('canvas');
  canvas.setAttribute('aria-hidden', 'true');
  canvas.style.cssText = `position:absolute;left:50%;top:50%;z-index:20;pointer-events:none;
    transform:translate(-50%,-50%);width:${Math.round(W * p.debordement)}px;height:${Math.round(H * p.debordement)}px;`;
  const cloth = creerCloth(canvas, { ...p, ratioCadre: W / H });
  if (!cloth) return false;
  try { await cloth.charger(url); } catch (_) { cloth.detruire(); return false; }

  ecran.appendChild(canvas);
  // L'écran réel disparaît à l'image près où le pli prend le relais.
  [...ecran.children].forEach(el => { if (el !== canvas) el.style.visibility = 'hidden'; });
  ecran.style.background = 'transparent';

  // `jouer()` ne va que du froissé vers le posé. La sortie est le trajet
  // INVERSE : on pilote `figer()` à la main, de 1 vers 0.
  return new Promise(res => {
    const t0 = performance.now();
    const courbe = bezier(...COURBE_SORTIE);
    const boucle = ts => {
      const t = Math.min(1, (ts - t0) / p.duree);
      cloth.figer(1 - courbe(t));
      canvas.style.opacity = String(Math.min(1, (1 - t) * 3.2));
      if (t < 1) requestAnimationFrame(boucle);
      else { canvas.remove(); cloth.detruire(); res(true); }
    };
    requestAnimationFrame(boucle);
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// LA SÉQUENCE
// Une bande VERTICALE de photogrammes défile dans une fenêtre fixe. Même
// mécanique dans les deux formats, seule la fenêtre change : l'écran entier en
// mobile, la moitié droite arrondie en desktop.
// ═══════════════════════════════════════════════════════════════════════════

// Attend que les <img> soient mesurables : c'est l'image qui donne le gabarit
// de la bande. Le fond en est EXCLU (il est lourd et hors du calcul) — le faire
// attendre ferait patienter devant du noir pour un décor.
const attendre = imgs => Promise.all([...imgs].map(img =>
  img.complete && img.naturalWidth ? null : new Promise(res => {
    img.addEventListener('load', res, { once: true });
    img.addEventListener('error', res, { once: true });
    setTimeout(res, 2000);
  })));

async function jouer(ecran, data, webglOk) {
  const mobile = matchMedia(`(max-width:${SEUIL_MOBILE}px)`).matches;
  const total = data.nouveautes.length;
  // Moins de trois nouveautés : on en montre autant qu'il y en a. Répéter la
  // même affiche pour tenir un compte fixe donnerait une transition entre deux
  // images identiques — un scintillement sans objet.
  const nouv = data.nouveautes.slice(0, AFFICHES_MAX);
  const n = nouv.length;

  // La durée n'est pas un réglage indépendant : elle EST la séquence. La poser
  // à part laisserait l'écran immobile sur la fin, ou couperait une affiche.
  const DUREE = n * POSE + (n - 1) * PAS;
  const cadence = PAS + POSE;

  const dDeb = new Date(data.debut + 'T12:00:00'), dFin = new Date(data.fin + 'T12:00:00');
  // Le jour de la semaine ne sert que sur la SECONDE ligne : « mercredi » à
  // l'ouverture n'apprend rien, « mardi » à la fermeture dit jusqu'à quand le
  // programme tient — c'est la seule des deux bornes qui engage le visiteur.
  const jourSeul = d => d.toLocaleDateString('fr-FR', { day: 'numeric', month: 'long' });
  const jourNomme = d => d.toLocaleDateString('fr-FR', { weekday: 'long', day: 'numeric', month: 'long' });

  const colonne = `
    <img class="fn-logo" src="${LOGO}" alt="CinéIndé Lyon">
    <div class="fn-ligne"><b>Semaine du</b> ${jourSeul(dDeb)}<br>au ${jourNomme(dFin)}</div>
    <div class="fn-bas">
      <div class="fn-nb"><b>${total}</b> sortie${total > 1 ? 's' : ''}</div>
      ${mobile ? '' : '<div class="fn-filet"><i></i></div>'}
    </div>
    ${mobile ? '<div class="fn-filet"><i></i></div>' : ''}`;

  ecran.innerHTML = mobile ? `
    <div class="fn-slot" style="--interimage:${INTERIMAGE}px">
      <div class="fn-bobine"></div>
      <div class="fn-lampe"></div>
    </div>
    <div class="fn-voile"></div>
    <div class="fn-txt">${colonne}</div>` : `
    <div class="fn-fond"><img src="${FOND}" alt=""></div>
    ${colonne}
    <div class="fn-cadre" style="--interimage:${INTERIMAGE}px">
      <div class="fn-bobine"></div>
      <div class="fn-lampe"></div>
    </div>`;

  const cadre  = ecran.querySelector(mobile ? '.fn-slot' : '.fn-cadre');
  const bobine = ecran.querySelector('.fn-bobine');
  const jauge  = ecran.querySelector('.fn-filet i');
  const fond   = ecran.querySelector('.fn-fond img');
  // Le fond arrive quand il arrive : il se pose en fondu plutôt que d'apparaître
  // d'un bloc au milieu de la séquence.
  if (fond) {
    const poser = () => fond.classList.add('vu');
    fond.complete && fond.naturalWidth ? poser() : fond.addEventListener('load', poser, { once: true });
  }

  nouv.forEach(f => {
    const ph = document.createElement('div');
    ph.className = 'fn-photo';
    ph.innerHTML = '<img alt="">';
    const img = ph.querySelector('img');
    img.onerror = () => { img.onerror = null; img.src = AFFICHE_REPLI; };
    img.src = f.poster || AFFICHE_REPLI;
    bobine.appendChild(ph);
  });

  await attendre(bobine.querySelectorAll('img'));

  // Le pas d'entraînement inclut l'interimage — sans elle la barre noire ne
  // traverserait jamais le cadre, et il ne resterait qu'un fondu de coupe.
  const H = cadre.clientHeight;
  const inter = parseFloat(getComputedStyle(bobine).gap) || INTERIMAGE;
  bobine.style.width = cadre.clientWidth + 'px';
  bobine.style.height = H + 'px';
  bobine.querySelectorAll('.fn-photo').forEach(ph => { ph.style.flex = `0 0 ${H}px`; });

  const pasPx = H + inter;

  let i = 0, sorti = false;

  // Le filet se remplit d'un trait sur toute la durée : c'est un compte à
  // rebours, pas une barre de chargement. C'est la contrepartie honnête d'un
  // écran qui retient — on montre toujours la sortie.
  jauge.style.transition = 'none'; jauge.style.width = '0';
  requestAnimationFrame(() => {
    jauge.style.transition = `width ${DUREE}ms linear`;
    jauge.style.width = '100%';
  });

  const avancer = () => {
    bobine.style.transition = `transform ${PAS}ms steps(7, end)`;
    bobine.style.transform = `translateY(${-i * pasPx}px)`;
    // Battement d'obturateur : la lampe ne délivre pas la même lumière d'un
    // photogramme à l'autre quand l'entraînement n'est plus synchrone.
    // Relancer les keyframes demande un reflow, sans quoi elle ne bat qu'une fois.
    cadre.style.setProperty('--t-saccade', PAS + 'ms');
    cadre.classList.remove('saccade'); void cadre.offsetWidth; cadre.classList.add('saccade');
  };

  // ── Ouverture ────────────────────────────────────────────────────────────
  // La première affiche est là d'emblée, sans amorce ni tirage. Le premier
  // changement vient au bout d'une POSE, pas d'une cadence — sinon la première
  // affiche resterait plus longtemps que les autres.
  let minuteur = null;
  avancer();
  const amorce = setTimeout(() => {
    if (sorti) return;
    tic();
    minuteur = setInterval(tic, cadence);
  }, POSE);

  // La sortie part pendant la pose de la dernière affiche.
  const echeance = setTimeout(sortir, DUREE);

  // On peut toujours passer : clic, molette ou touche emportent l'écran.
  ecran.addEventListener('click', sortir);
  addEventListener('wheel', sortir, { passive: true, once: true });
  addEventListener('keydown', sortir, { once: true });

  function tic() {
    i++;
    // La sortie a déjà la main : il n'y a plus rien à avancer, surtout pas une
    // affiche de plus.
    if (i >= n) { clearInterval(minuteur); return; }
    avancer();
  }

  // La promesse est créée AVANT `sortir` : c'est elle qui porte le `resolve`
  // que la sortie appellera, et `sortir` est armé dès la première image.
  let resoudre;
  const finie = new Promise(r => { resoudre = r; });

  async function sortir() {
    if (sorti) return; sorti = true;
    clearTimeout(amorce); clearTimeout(echeance); clearInterval(minuteur);
    resoudre(await sortirEnTissu(ecran, webglOk));
  }

  return finie;
}

// ═══════════════════════════════════════════════════════════════════════════
// AMORÇAGE
// ═══════════════════════════════════════════════════════════════════════════
/**
 * Joue l'écran d'arrivée puis le retire. Ne rejette jamais : quoi qu'il arrive
 * (réseau muet, WebGL absent, capture teintée), l'écran s'efface et le site
 * reprend la main.
 *
 * @param {{client:object, url:string, cle:string}} cfg — le client Supabase UMD
 *        et ses clés, passés explicitement par index.html.
 */
export async function lancerIntro(cfg) {
  const ecran = document.getElementById('introEcran');
  if (!ecran) return;

  let fini = false;
  const retirer = (fondu) => {
    if (fini) return; fini = true;
    document.documentElement.classList.remove('intro-on');
    if (!fondu) { ecran.remove(); return; }
    ecran.classList.add('parti');
    setTimeout(() => ecran.remove(), 700);
  };

  // Filet : si les données tardent, l'écran s'efface sans rien avoir montré.
  // Mieux vaut pas d'intro qu'une intro qui fait attendre.
  const garde = setTimeout(() => retirer(true), ATTENTE_MAX);

  // WebGL2 est requis par le pli de sortie. Sans lui, la séquence joue quand
  // même et c'est le fondu CSS qui emporte l'écran.
  const webglOk = (() => {
    try { return !!document.createElement('canvas').getContext('webgl2'); } catch { return false; }
  })();

  let data;
  try {
    data = await chargerNouveautes(cfg);
  } catch (e) {
    console.warn('[CinémasLyon] intro : données indisponibles —', e?.message || e);
    clearTimeout(garde); retirer(true); return;
  }
  // Une semaine sans nouveauté n'a rien à annoncer : l'écran se saute.
  if (fini || !data.nouveautes.length) { clearTimeout(garde); retirer(true); return; }
  clearTimeout(garde);

  let plie = false;
  try { plie = await jouer(ecran, data, webglOk); }
  catch (e) { console.warn('[CinémasLyon] intro interrompue —', e?.message || e); }
  // Le pli a déjà fait disparaître l'écran : le retirer en fondu rejouerait une
  // seconde disparition par-dessus la première.
  retirer(!plie);
}
