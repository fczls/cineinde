// Tests des fonctions PURES de l'onglet Évènements (chip de date, tri, tirage).
//
//     node tests/chip_dates.test.mjs
//
// Le front est un fichier unique assemblé par build_ui.py : plutôt que d'en
// extraire un module (ce qui casserait la contrainte mono-fichier), on lit le
// bloc de fonctions pures délimité par `@test-block` dans src/template.html et
// on l'évalue avec ses quelques dépendances injectées. Aucune dépendance npm.
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import assert from 'node:assert/strict';

const racine = join(dirname(fileURLToPath(import.meta.url)), '..');
const source = readFileSync(join(racine, 'src', 'template.html'), 'utf8');
const bloc = source.split('// @test-block:start')[1]?.split('// @test-block:end')[0]
  // la ligne du marqueur porte un commentaire de fin de ligne : on la neutralise
  ?.replace(/^[^\n]*\n/, '\n');
assert.ok(bloc, 'bloc @test-block introuvable dans src/template.html');

// Dépendances du bloc, injectées : le reste du fichier n'est pas chargé.
const deps = {
  MOIS_NOMS: ['janvier','février','mars','avril','mai','juin',
              'juillet','août','septembre','octobre','novembre','décembre'],
  evAffiche: () => true,   // éligibilité « a une affiche » — vraie dans les tests
  // Date du jour figée : les règles qui comparent à « aujourd'hui » (chip
  // `en_cours`) doivent être testables sans dépendre du calendrier.
  dKey: () => '2026-08-15',
  // Normalisation de titre — version simplifiée suffisante ici : la dédup ne
  // demande qu'une comparaison insensible à la casse et aux accents.
  normalizeTitle: t => (t || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '')
                                .toLowerCase().replace(/\s+/g, ' ').trim(),
};
const api = new Function(...Object.keys(deps),
  `${bloc}\n return { eventDateChip, evChipDates, sortEvents, monthsWithEvents, pickSelection, dedupeEvents };`
)(...Object.values(deps));

const { eventDateChip, evChipDates, sortEvents, monthsWithEvents, pickSelection, dedupeEvents } = api;

let vert = 0;
const test = (nom, fn) => {
  try { fn(); vert++; console.log(`  ✓ ${nom}`); }
  catch (e) { console.error(`  ✗ ${nom}\n    ${e.message}`); process.exitCode = 1; }
};

// ── Les six formes de la chip (§7.2) ────────────────────────────────────
console.log('chip de date — les six formes');

test('date unique dans le mois affiché → « 30 »', () => {
  assert.equal(eventDateChip(['2026-07-30'], '2026-07'), '30');
});

test('deux dates dans le mois affiché → « 20 & 24 »', () => {
  assert.equal(eventDateChip(['2026-08-20', '2026-08-24'], '2026-08'), '20 & 24');
});

test('période dans le mois affiché → « 25 → 31 »', () => {
  assert.equal(eventDateChip(['2026-07-25', '2026-07-31'], '2026-07', { mode: 'periode' }),
               '25 → 31');
});

test('période à cheval sur deux mois → « 28 juin → 18 août »', () => {
  assert.equal(eventDateChip(['2026-06-28', '2026-08-18'], '2026-07', { mode: 'periode' }),
               '28 juin → 18 août');
});

test('precision saison → « Cet été »', () => {
  assert.equal(eventDateChip([], '2026-07', { precision: 'saison' }), 'Cet été');
});

test('precision en_cours → « En cours »', () => {
  assert.equal(eventDateChip([], '2026-07', { precision: 'en_cours' }), 'En cours');
});

// ── Règle du mois omis : la source d'incohérence la plus probable ────────
console.log('règle du mois omis');

test('une date hors du mois affiché porte son mois', () => {
  assert.equal(eventDateChip(['2026-08-03'], '2026-07'), '3 août');
});

test('deux dates hors du mois affiché portent leur mois', () => {
  assert.equal(eventDateChip(['2026-08-20', '2026-09-02'], '2026-07'), '20 août & 2 septembre');
});

test('au niveau 2, le mois est TOUJOURS écrit', () => {
  assert.equal(eventDateChip(['2026-07-30'], '2026-07', { alwaysMonth: true }), '30 juillet');
  assert.equal(eventDateChip(['2026-07-15', '2026-07-24'], null, { alwaysMonth: true, et: true }),
               '15 et 24 juillet');
});

test('trois dates ou plus basculent sur la période (§9.3)', () => {
  assert.equal(
    eventDateChip(['2026-07-15', '2026-07-16', '2026-07-21'], null,
                  { alwaysMonth: true, mode: 'periode' }),
    '15 → 21 juillet');
});

// ── Dates dérivées du périmètre (§8.2) ──────────────────────────────────
console.log('chip dérivée des créneaux filtrés');

const inconnue = {
  date_debut: '2026-08-20', date_fin: '2026-08-24', precision: 'exact',
  creneaux: [
    { cinema: 'Le Comoedia', date: '2026-08-20' },
    { cinema: 'Lumière Terreaux', date: '2026-08-24' },
  ],
};

test('sans filtre : « 20 & 24 »', () => {
  const c = evChipDates(inconnue, 'tous');
  assert.equal(eventDateChip(c.dates, '2026-08', { mode: c.mode }), '20 & 24');
});

test('filtré sur Comoedia : « 20 »', () => {
  const c = evChipDates(inconnue, 'Le Comoedia');
  assert.equal(eventDateChip(c.dates, '2026-08', { mode: c.mode }), '20');
});

test('filtre sur la seule salle de l’évènement : l’enveloppe reste lisible', () => {
  // Le créneau n'est pas daté (la source ne l'a pas donné) mais la période est
  // connue : afficher « En cours » ici serait une perte d'information.
  const retro = {
    date_debut: '2026-08-12', date_fin: '2026-08-30', precision: 'exact',
    creneaux: [{ cinema: 'Lumière Bellecour', date: null }],
  };
  const c = evChipDates(retro, 'Lumière Bellecour');
  assert.equal(eventDateChip(c.dates, '2026-08', { mode: c.mode }), '12 → 30');
});

test('filtre sur une salle parmi plusieurs : l’enveloppe est écartée', () => {
  const c = evChipDates(inconnue, 'Le Comoedia');
  assert.deepEqual(c.dates, ['2026-08-20']);
});

// « En cours » ne cadre rien pour le lecteur : même sans fin annoncée par la
// source, la chip doit porter les dates réellement connues.
test('en_cours à dates connues → ces dates, jamais « En cours »', () => {
  const ressortie = {
    precision: 'en_cours', date_debut: '2026-08-19', date_fin: '2026-08-25',
    creneaux: [{ cinema: 'Lumière Bellecour', date: '2026-08-19' },
               { cinema: 'Lumière Bellecour', date: '2026-08-25' }],
  };
  const c = evChipDates(ressortie, 'tous');
  assert.equal(eventDateChip(c.dates, '2026-08', { mode: c.mode, precision: 'en_cours' }),
               '19 & 25');
});

test('en_cours sur plusieurs séances → la période connue', () => {
  const ressortie = {
    precision: 'en_cours', date_debut: '2026-07-29', date_fin: '2026-08-04',
    creneaux: [{ cinema: 'Lumière Terreaux', date: '2026-08-01' }],
  };
  const c = evChipDates(ressortie, 'tous');
  assert.equal(eventDateChip(c.dates, '2026-08', { mode: c.mode, precision: 'en_cours' }),
               '29 juillet → 4 août');
});

test('saison → « Cet été », même si la jointure a rempli des dates', () => {
  const retro = {
    precision: 'saison', date_debut: '2026-07-15', date_fin: '2026-08-12',
    creneaux: [{ cinema: 'Lumière Bellecour', date: '2026-08-01' }],
  };
  const c = evChipDates(retro, 'tous');
  assert.equal(eventDateChip(c.dates, '2026-08', { mode: c.mode, precision: 'saison' }),
               'Cet été');
});

test('un festival dont une seule séance est connue reste une période', () => {
  const fest = {
    date_debut: '2026-07-01', date_fin: '2026-09-01', precision: 'exact',
    creneaux: [{ cinema: 'Le Comoedia', date: '2026-07-01' }],
  };
  const c = evChipDates(fest, 'tous');
  assert.equal(c.mode, 'periode');
  assert.equal(eventDateChip(c.dates, '2026-07', { mode: c.mode }), '1 juillet → 1 septembre');
});

// ── Tri de la liste (§7.3) ──────────────────────────────────────────────
console.log('tri de la liste');

test('bloc large en tête (durée décroissante), puis bloc daté', () => {
  const events = [
    { titre: 'AVP', date_debut: '2026-08-20', date_fin: '2026-08-20', precision: 'exact' },
    { titre: 'Festival long', date_debut: '2026-06-01', date_fin: '2026-10-01', precision: 'exact' },
    { titre: 'Cycle', date_debut: '2026-07-20', date_fin: '2026-09-05', precision: 'exact' },
    { titre: 'Rencontre', date_debut: '2026-08-02', date_fin: '2026-08-02', precision: 'exact' },
  ];
  assert.deepEqual(sortEvents(events, '2026-08').map(e => e.titre),
                   ['Festival long', 'Cycle', 'Rencontre', 'AVP']);
});

test('sans date_debut, date_fin sert de clé de tri', () => {
  const events = [
    { titre: 'B', date_debut: '2026-08-10', date_fin: '2026-08-10', precision: 'exact' },
    { titre: 'A', date_debut: null, date_fin: '2026-08-05', precision: 'exact' },
  ];
  assert.deepEqual(sortEvents(events, '2026-08').map(e => e.titre), ['A', 'B']);
});

// ── Mois navigables (§8.4) ──────────────────────────────────────────────
console.log('mois navigables');

const troisMois = [
  { titre: 'A', type: 'festival', date_debut: '2026-08-01', date_fin: '2026-08-10',
    creneaux: [{ cinema: 'Le Comoedia', date: '2026-08-01' }] },
  { titre: 'B', type: 'avant_premiere', date_debut: '2026-10-02', date_fin: '2026-10-02',
    creneaux: [{ cinema: 'Lumière Terreaux', date: '2026-10-02' }] },
];

test('un mois sans évènement est absent de la liste (septembre)', () => {
  assert.deepEqual(monthsWithEvents(troisMois, 'tous', 'tous', '2026-08-01'),
                   ['2026-08', '2026-10']);
});

test('les mois passés ne sont pas navigables', () => {
  assert.deepEqual(monthsWithEvents(troisMois, 'tous', 'tous', '2026-09-15'), ['2026-10']);
});

test('la liste des mois suit les filtres actifs', () => {
  assert.deepEqual(monthsWithEvents(troisMois, 'Le Comoedia', 'tous', '2026-08-01'), ['2026-08']);
  assert.deepEqual(monthsWithEvents(troisMois, 'tous', 'avant_premiere', '2026-08-01'), ['2026-10']);
});

// ── Tirage de la sélection (§6.1) ───────────────────────────────────────
console.log('tirage de la sélection');

const faux = n => Array.from({ length: n }, (_, i) => ({ cle: `ev-${i}`, date_debut: '2026-08-01' }));

test('les paliers : 1⇒1, 3⇒1, 5⇒3, 9⇒5', () => {
  assert.equal(pickSelection(faux(1), 'graine', 'scope').length, 1);
  assert.equal(pickSelection(faux(3), 'graine', 'scope').length, 1);
  assert.equal(pickSelection(faux(5), 'graine', 'scope').length, 3);
  assert.equal(pickSelection(faux(9), 'graine', 'scope').length, 5);
});

test('le tirage est stable à graine et périmètre constants', () => {
  const a = pickSelection(faux(9), 'graine', 'scope').map(e => e.cle);
  const b = pickSelection(faux(9), 'graine', 'scope').map(e => e.cle);
  assert.deepEqual(a, b);
});

test('changer de graine change le tirage', () => {
  const a = pickSelection(faux(9), 'graine-1', 'scope').map(e => e.cle);
  const b = pickSelection(faux(9), 'graine-2', 'scope').map(e => e.cle);
  assert.notDeepEqual(a, b);
});

test('le nombre suit le périmètre filtré, pas un tirage global', () => {
  // 5 éligibles dans le périmètre ⇒ 3 cartes, quoi qu'il arrive hors périmètre.
  assert.equal(pickSelection(faux(5), 'graine', 'Le Comoedia|tous|2026-08').length, 3);
});

// ── Déduplication des évènements ──────────────────────────────────────────
// Régression : Supabase porte plusieurs lignes pour un même évènement, la clé
// d'idempotence du scraper s'ancrant sur le mois de DÉBUT (qui glisse quand un
// évènement long traverse un mois) ou sur une identité qui change selon que la
// ligne porte 0 ou 1 film.
const ev = (o) => Object.assign(
  { type: 'festival', titre: 'X', date_debut: null, date_fin: null, films: [], creneaux: [] }, o);

test('fusionne deux lignes du même cycle dont le début a glissé', () => {
  const out = dedupeEvents([
    ev({ titre: 'Cycle Scary Fourmi', date_debut: '2026-07-04', date_fin: '2026-08-22',
         films: [{ titre: 'The Thing' }], creneaux: [{ cinema: 'F', date: '2026-08-02', heure: '20:00' }] }),
    ev({ titre: 'Cycle Scary Fourmi', date_debut: '2026-08-01', date_fin: '2026-08-22',
         films: [{ titre: 'La Mouche' }], creneaux: [{ cinema: 'F', date: '2026-08-09', heure: '20:00' }] }),
  ]);
  assert.equal(out.length, 1);
  assert.equal(out[0].date_debut, '2026-07-04', 'garde le début le plus tôt');
  assert.equal(out[0].date_fin, '2026-08-22');
  assert.equal(out[0].films.length, 2, 'réunit les films des deux lignes');
  assert.equal(out[0].creneaux.length, 2, 'réunit les créneaux des deux lignes');
});

test('la ligne vide est absorbée par celle qui porte le contenu', () => {
  const out = dedupeEvents([
    ev({ titre: 'Résonance - Biennale', date_debut: '2026-09-14', date_fin: '2026-09-14' }),
    ev({ titre: 'Résonance - Biennale', date_debut: '2026-09-14', date_fin: '2026-09-14',
         films: [{ titre: 'Les Glaneurs' }], creneaux: [{ cinema: 'C', date: '2026-09-14', heure: '19:00' }] }),
  ]);
  assert.equal(out.length, 1);
  assert.equal(out[0].films.length, 1);
  assert.equal(out[0].creneaux.length, 1);
});

test('ne fusionne pas deux éditions aux périodes disjointes', () => {
  const out = dedupeEvents([
    ev({ titre: 'Festival Play It Again !', date_debut: '2026-09-01', date_fin: '2026-09-07' }),
    ev({ titre: 'Festival Play It Again !', date_debut: '2027-09-01', date_fin: '2027-09-07' }),
  ]);
  assert.equal(out.length, 2, 'deux éditions annuelles restent distinctes');
});

test('ne fusionne pas deux types différents au même titre', () => {
  const out = dedupeEvents([
    ev({ type: 'festival', titre: 'Akira', date_debut: '2026-09-10', date_fin: '2026-09-10' }),
    ev({ type: 'avant-premiere', titre: 'Akira', date_debut: '2026-09-10', date_fin: '2026-09-10' }),
  ]);
  assert.equal(out.length, 2);
});

test('les créneaux identiques ne sont pas dupliqués par la fusion', () => {
  const c = { cinema: 'F', date: '2026-08-02', heure: '20:00', titre_film: 'The Thing' };
  const out = dedupeEvents([
    ev({ titre: 'Cycle', date_debut: '2026-08-01', date_fin: '2026-08-30', creneaux: [c] }),
    ev({ titre: 'Cycle', date_debut: '2026-08-01', date_fin: '2026-08-30', creneaux: [{ ...c }] }),
  ]);
  assert.equal(out.length, 1);
  assert.equal(out[0].creneaux.length, 1, 'même signature = un seul créneau');
});

console.log(`\n${vert} tests OK`);
