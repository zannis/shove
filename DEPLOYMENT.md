# Deployment

`https://shove.rs` is served by Cloudflare Pages from the `main` branch of this repo.

## One-time Cloudflare setup

1. Cloudflare dashboard → **Workers & Pages → Create application → Pages → Connect to Git**.
2. Select the `zannis/shove` repository.
3. **Project name:** `shove-docs` (yields `shove-docs.pages.dev`).
4. **Production branch:** `main`.
5. **Framework preset:** None. Configure manually:
   - Build command: `npm install && npm run build`
   - Build output directory: `docs/dist`
   - Root directory: `/`
   - Environment variable: `NODE_VERSION=22`
6. **Save & deploy.** First build runs against current `main`. Verify on `shove-docs.pages.dev`.

## Custom domain (`shove.rs`)

Assumes DNS is on Cloudflare (move it if not — Cloudflare's free DNS plan).

1. Cloudflare Pages project → **Custom domains → Add `shove.rs`**. SSL provisions automatically (~1 minute).
2. Add `www.shove.rs` as a second custom domain.
3. Cloudflare Rules → **Page Rules**: `www.shove.rs/*` → `https://shove.rs/$1` (301 redirect).

## PR previews

Cloudflare Pages auto-deploys every PR to `<branch-name>.shove-docs.pages.dev`. The `.github/workflows/docs-preview.yml` workflow comments the preview URL on the PR.

## Build settings reference

See `wrangler.toml` for the build settings as committed source-of-truth.

## Rebuilding from scratch

If the Cloudflare account is lost, follow steps above. All other config (logos, vocs, sidebar) lives in this repo.
