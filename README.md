# switch-ext-3d-analysis

3D analysis of parts and models: printability geometry, FEA and CFD on rented Hetzner boxes, photoreal Blender renders, and sizes measured off a listing's photos.

A [Switch](https://switch.ax) extension: a Python MCP server (`sbmcp.py`, Switchboard appendages merged into one) that Switch runs through [switch-ext-kit](https://github.com/zachsents/switch-ext-kit)'s shim. Its tools show as `analysis_<tool>`.

- Mesh (`mesh.py`): `report`, `section`, `view`, `layers`, `supports`, `support_model`, `convert`, `scene`, `coupon`, `variants`, `drawing`, and `set_spec` / `spec` / `verify` for a job's sliced plates. It also writes `~/.local/bin/mesh-python`, a Python with the same pinned geometry packages.
- FEA (`fea_*`): gmsh and CalculiX on a temporary Hetzner box.
- CFD (`cfd_*`): an OpenFOAM case matrix on a temporary Hetzner box.
- Render (`render`, `render_scenes`): Blender Cycles in a standard scene, with an optional AI print-look pass.
- Photos (`photos`, `measure`, `look`): a listing's photos and a part's size measured off a photo.

`fea_run`, `fea_add`, `cfd_start`, `cfd_add` and `cfd_extend` rent or keep a paid box, so they speak for the user. The background loop only watches, fetches results and deletes the extension's own boxes once they're done or past their delete-by.

## Install

Ask Switch's agent to install `github:zachsents/switch-ext-3d-analysis`, or add it in the desktop's Extensions view (⌘K, Extensions). Switch runs it on a machine with what it needs (`macos`).

## Setup

The machine must have:

- `uv` (`brew install uv`). Mesh builds its pinned environment (trimesh with Embree, numpy, scipy, shapely, matplotlib, manifold3d, cascadio...) with it on first start, and photo work adds pyobjc's Vision and Quartz to it.
- Blender (`brew install --cask blender`), for `render` and for reading .blend, FBX and USD files in `convert` and `scene`.
- `ssh`, `ssh-keygen` and `tar`, for the Hetzner boxes (macOS has them).
- switch-ext-browser (`github:zachsents/switch-ext-browser#a4e1fda` or later), for `photos` on pages that turn curl away. It uses the user's own browser and never solves a human check.

Connections:

- Hetzner Cloud (`hetzner`, a key): an API token for the project boxes are rented in. FEA and CFD need it.
- AI Gateway (`ai-gateway`, a key): a Vercel AI Gateway key, for `render`'s `ai_look` (about $0.05-0.14 a shot). Without it, everything else still works.

A key kept from before connections moves in once: put `keys.json` (`{"hetzner": "…", "ai-gateway": "…"}`, 0600) in the extension's home; the first time one is needed it goes to the user's connections (account `default`) and leaves the file.

`config.json` in the extension's home, every key optional:

```json
{ "bed_mm": 256, "blender": "/Applications/Blender.app/Contents/MacOS/Blender", "results": "~/analysis-results" }
```

- `bed_mm`: the printer's bed, each way (default 256).
- `blender`: Blender's path, when it isn't in /Applications or on PATH.
- `results`: where FEA and CFD results land (`fea/<study>` and `cfd/<study>` under it). The default is `results/` in each part's home.

Everything else lives in the home too: each call's pictures and files (`mesh/out`, kept a week), render jobs, specs, studies, and the ssh key the boxes let in.
