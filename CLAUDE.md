# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Research toolset for scanning municipal government websites in Minas Gerais (853 municipalities) to identify OPM — *Organismos de Políticas para Mulheres* (Women's Policy Bodies). Uses the Google Custom Search API (100 free queries/day limit).

## Setup

```bash
bundle install
```

To regenerate the input CSV from IBGE's API (already committed as `municipios_minas_gerais.csv`):
```bash
ruby gerar_csv_municipios.rb
```

## Running the Scanners

Each scanner is a standalone script. The main one is:

```bash
ruby scanner_prefeituras_opm.rb
```

Other scanners:
```bash
ruby scanner_google_custom_search_realtime.rb   # alternative approach, lower query limit
ruby scanner_diario_oficial_opm.rb              # searches Diários Oficiais for OPM portarias
ruby scanner_instagram_google_search.rb         # searches prefeitura Instagram accounts
```

## Architecture

### Common Pattern

All scanners share the same structure:
1. Load municipalities from `municipios_minas_gerais.csv` (columns: `nome`, `codigo_ibge`)
2. For each municipality: make 2 Google Custom Search API calls — one to find the prefeitura website, one to search for OPM keywords within that site
3. Write results incrementally to CSV and JSON after each municipality

### Test Mode

Every scanner has a `LIMITE_MUNICIPIOS` constant. Set to an integer (e.g. `5`) for a quick test run; set to `nil` for all 853 municipalities.

### Rate Limiting

- `QUERIES_POR_DIA = 100` — free API tier limit
- `DELAY_ENTRE_QUERIES = 0.9` seconds — applied between each municipality
- Each municipality consumes ~2 queries (find site + search OPM), so full runs require multiple days

### Checkpoint / Resume (main scanner only)

`scanner_prefeituras_opm.rb` persists progress to `dados/checkpoint.json`. If interrupted, rerunning it skips already-processed municipalities automatically. The checkpoint is deleted upon successful completion.

### Output Files

The main scanner writes to `./dados/`:
- `prefeituras_opm_resultado.csv` — one row per municipality, updated incrementally
- `prefeituras_opm_resultado.json` — full data with summary stats
- `prefeituras_opm_resultado_formatado.json` — split into `municipios_com_opm` / `municipios_sem_opm` with keyword frequency analysis
- `scanner.log` — timestamped INFO/ERROR log

Other scanners write output to the current directory.

### API Credentials

`GOOGLE_API_KEY` and `SEARCH_ENGINE_ID` are hardcoded constants in each scanner file. All scanners use `https://www.googleapis.com/customsearch/v1` via the `HTTParty` gem.

### OPM Keywords

Defined as `OPM_KEYWORDS` in each scanner — Portuguese terms for women's policy structures (secretaria, coordenadoria, diretoria, núcleo, conselho, etc.). The main scanner uses ~30 keywords joined with `OR` in the `site:` scoped query.
