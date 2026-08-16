import { readdirSync, statSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

// Written into docs/public (a committed source asset) rather than docs/dist, so
// vocs copies it out on any build command and the file ships even if the
// deploy pipeline calls `vocs build` directly instead of `pnpm build`.
const siteUrl = 'https://shove.rs'
const pagesDir = resolve(import.meta.dirname, '../docs/pages')
const outFile = resolve(import.meta.dirname, '../docs/public/sitemap.xml')

// Mirrors vocs' own route derivation (see vocs/_lib/vite/prerender.js).
function collectRoutes(dir) {
  const routes = []
  for (const entry of readdirSync(dir)) {
    const path = resolve(dir, entry)
    if (statSync(path).isDirectory()) {
      routes.push(...collectRoutes(path))
      continue
    }
    if (!/\.mdx?$/.test(entry)) continue
    const route = path.replace(pagesDir, '').replace(/\.[^.]*$/, '')
    routes.push(route.endsWith('/index') ? route.replace(/index$/, '') : route)
  }
  return routes
}

const urls = collectRoutes(pagesDir)
  .sort()
  .map((route) => `  <url><loc>${siteUrl}${route}</loc></url>`)
  .join('\n')

writeFileSync(
  outFile,
  `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>
`,
)

console.log(`sitemap.xml written with ${urls.split('\n').length} urls`)
