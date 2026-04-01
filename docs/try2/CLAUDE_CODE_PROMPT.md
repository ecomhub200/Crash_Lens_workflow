I have 8 files saved in docs/try2/ that need to be placed in specific locations across 2 repos. Do NOT modify file contents — just copy/move them to the correct paths.

## Repo: ecomhub200/Douglas_County_2 (main repo — shared pipeline modules)

### REPLACE existing file:
```
docs/try2/build_road_inventory.py → build_road_inventory.py
```
This replaces the existing build_road_inventory.py at root. It adds:
- enrich_state_dot() function (Tier A State DOT enrichment)
- state_dot added to file_map for loading
- hpms_ownership preserved (not dropped)
- road_inventory_postprocess integration
- Updated header documenting Tier A/B/C/D hierarchy

### NEW files at root:
```
docs/try2/road_inventory_postprocess.py → road_inventory_postprocess.py
docs/try2/generate_state_dot_data.py → generate_state_dot_data.py
docs/try2/patch_road_data_authority.py → patch_road_data_authority.py
```

### PATCH existing file:
After copying patch_road_data_authority.py, run it against the existing road_data_authority.py:
```bash
python patch_road_data_authority.py road_data_authority.py
```
This adds State DOT as Tier A in resolve_speed_limit, resolve_lanes, resolve_surface, and merge_frontend_columns. After patching, you can delete patch_road_data_authority.py.

## Repo: ecomhub200/Crash_Lens_workflow (public workflows)

### NEW files — create directories if needed:
```
docs/try2/states__init__.py → states/__init__.py
docs/try2/states_delaware__init__.py → states/delaware/__init__.py
docs/try2/de_state_dot.py → states/delaware/de_state_dot.py
docs/try2/generate-state-dot-data.yml → .github/workflows/generate-state-dot-data.yml
```
Both __init__.py files are empty (package markers for Python imports).

## Summary of commands:

```bash
# In Douglas_County_2 repo:
cp docs/try2/build_road_inventory.py build_road_inventory.py
cp docs/try2/road_inventory_postprocess.py road_inventory_postprocess.py
cp docs/try2/generate_state_dot_data.py generate_state_dot_data.py
cp docs/try2/patch_road_data_authority.py patch_road_data_authority.py
python patch_road_data_authority.py road_data_authority.py
rm patch_road_data_authority.py

# In Crash_Lens_workflow repo:
mkdir -p states/delaware
touch states/__init__.py
touch states/delaware/__init__.py
cp docs/try2/de_state_dot.py states/delaware/de_state_dot.py
cp docs/try2/generate-state-dot-data.yml .github/workflows/generate-state-dot-data.yml
```

## File manifest:
| File | Lines | Destination | Action |
|------|-------|-------------|--------|
| build_road_inventory.py | 1923 | Douglas_County_2/ | REPLACE |
| road_inventory_postprocess.py | 996 | Douglas_County_2/ | NEW |
| generate_state_dot_data.py | 485 | Douglas_County_2/ | NEW |
| patch_road_data_authority.py | 351 | Douglas_County_2/ (temp) | RUN then DELETE |
| de_state_dot.py | 404 | Crash_Lens_workflow/states/delaware/ | NEW |
| generate-state-dot-data.yml | 111 | Crash_Lens_workflow/.github/workflows/ | NEW |
| states__init__.py | 0 | Crash_Lens_workflow/states/ | NEW |
| states_delaware__init__.py | 0 | Crash_Lens_workflow/states/delaware/ | NEW |
